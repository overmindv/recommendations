package catalog

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type graph struct {
	nodes    map[[16]byte]int
	outgoing [][]int
	incoming []int
}

func (g *graph) node(id [16]byte) int {
	if i, exists := g.nodes[id]; exists {
		return i
	}
	i := len(g.outgoing)
	g.nodes[id] = i
	g.outgoing = append(g.outgoing, nil)
	g.incoming = append(g.incoming, 0)

	return i
}

func (g *graph) add(source, target [16]byte) {
	i, j := g.node(source), g.node(target)
	g.outgoing[i] = append(g.outgoing[i], j)
	g.incoming[j]++
}

func (g *graph) acyclic() bool {
	queue := make([]int, 0, len(g.incoming))
	for i, degree := range g.incoming {
		if degree == 0 {
			queue = append(queue, i)
		}
	}
	for head := 0; head < len(queue); head++ {
		for _, next := range g.outgoing[queue[head]] {
			g.incoming[next]--
			if g.incoming[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	return len(queue) == len(g.incoming)
}

func validateGraph(ctx context.Context, tx pgx.Tx, generation int64) error {
	rows, err := tx.Query(ctx, `SELECT e.topic_id,e.prerequisite_id,(t.topic_id IS NULL OR p.topic_id IS NULL)
		FROM catalog_projected_prerequisites e
		LEFT JOIN catalog_projected_topics t ON t.generation=e.generation AND t.topic_id=e.topic_id
		LEFT JOIN catalog_projected_topics p ON p.generation=e.generation AND p.topic_id=e.prerequisite_id
		WHERE e.generation=$1 AND NOT e.tombstone
		  AND (t.topic_id IS NULL OR p.topic_id IS NULL OR NOT (t.tombstone OR p.tombstone))
		LIMIT $2`, generation, maxGraphEdges+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	g := graph{nodes: make(map[[16]byte]int)}
	n := 0
	for rows.Next() {
		n++
		if n > maxGraphEdges {
			return invalid("too_many_live_edges")
		}
		var from, to pgtype.UUID
		var missing bool
		if err := rows.Scan(&from, &to, &missing); err != nil {
			return err
		}
		if missing {
			return invalid("missing_prerequisite_topic")
		}
		g.add(from.Bytes, to.Bytes)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !g.acyclic() {
		return invalid("prerequisite_cycle")
	}

	return nil
}

const validateEdgeSQL = `WITH RECURSIVE edge AS (
			SELECT e.prerequisite_id,(t.topic_id IS NULL OR p.topic_id IS NULL) AS missing,
			       NOT (t.tombstone OR p.tombstone) AS live
			FROM catalog_projected_prerequisites e
			LEFT JOIN catalog_projected_topics t ON t.generation=e.generation AND t.topic_id=e.topic_id
			LEFT JOIN catalog_projected_topics p ON p.generation=e.generation AND p.topic_id=e.prerequisite_id
			WHERE e.generation=$1 AND e.topic_id=$2 AND e.prerequisite_id=$3 AND NOT e.tombstone
		), walk(id) AS (
			SELECT prerequisite_id FROM edge WHERE live AND NOT missing
			UNION
			SELECT e.prerequisite_id FROM walk w
			JOIN catalog_projected_prerequisites e ON e.generation=$1 AND e.topic_id=w.id AND NOT e.tombstone
			JOIN catalog_projected_topics p ON p.generation=$1 AND p.topic_id=e.prerequisite_id AND NOT p.tombstone
			WHERE w.id<>$2
		)
		SELECT COALESCE((SELECT missing FROM edge),false), EXISTS(SELECT 1 FROM walk WHERE id=$2)`

func validateEdges(ctx context.Context, tx pgx.Tx, generation int64, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, event := range events {
		batch.Queue(validateEdgeSQL, generation, event.Record.TopicID, event.Record.PrerequisiteID)
	}
	results := tx.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()
	for _, event := range events {
		var missing, cycle bool
		if err := results.QueryRow().Scan(&missing, &cycle); err != nil {
			return err
		}
		if missing {
			return markRejected(invalid("missing_prerequisite_topic"), event.Revision, event.ID)
		}
		if cycle {
			return markRejected(invalid("prerequisite_cycle"), event.Revision, event.ID)
		}
	}

	return results.Close()
}
