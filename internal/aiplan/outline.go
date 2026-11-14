package aiplan

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/overmindv/recommendations/internal/planner"
)

// OutlineRequest содержит только публичные темы и явно отправленную цель.
type OutlineRequest struct {
	CompletedMaterialIDs []string        `json:"completed_material_ids,omitempty"`
	Goal                 string          `json:"goal"`
	Topics               []planner.Topic `json:"topics"`
}
type Material struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Action  string `json:"action"`
	Minutes int    `json:"minutes"`
}
type Outline struct {
	AIStatus  string     `json:"ai_status"`
	TopicIDs  []string   `json:"topic_ids"`
	Materials []Material `json:"materials"`
}

// Редакционный каталог: модель выбирает ID, ссылки никогда не генерирует.
var library = []Material{
	{"python", "Python: основы языка", "https://docs.python.org/3/tutorial/", "Изучите списки, словари и функции; напишите функцию подсчёта частот слов и объясните её сложность.", 25},
	{"numpy", "NumPy: массивы и векторизация", "https://numpy.org/doc/stable/user/absolute_beginners.html", "Создайте массив, выполните broadcasting и объясните отличие от цикла Python.", 25},
	{"pandas", "pandas: подготовка данных", "https://pandas.pydata.org/docs/getting_started/intro_tutorials/", "Загрузите небольшой CSV, найдите пропуски и разделите признаки и целевую переменную.", 30},
	{"ml", "Machine Learning: регрессия и классификация", "https://scikit-learn.org/stable/getting_started.html", "Обучите baseline-модель с train/test split и объясните, почему нельзя оценивать её на обучающей выборке.", 40},
	{"evaluation", "Метрики и кросс-валидация", "https://scikit-learn.org/stable/modules/cross_validation.html", "Сравните holdout и cross-validation; найдите пример утечки данных и объясните выбор метрики.", 30},
	{"torch", "PyTorch: нейросети", "https://pytorch.org/tutorials/beginner/basics/intro.html", "Пройдите вводный шаг с тензорами и autograd; объясните прямой и обратный проход.", 40},
	{"nlp", "NLP, Transformers и LLM", "https://huggingface.co/learn/llm-course/chapter1/1", "Изучите введение, запустите pipeline и объясните токенизацию и отличие inference от обучения.", 30},
	{"cv", "Computer Vision: перенос обучения", "https://pytorch.org/tutorials/beginner/transfer_learning_tutorial.html", "Разберите пример классификации изображений; объясните augmentation и заморозку весов.", 40},
	{"deployment", "FastAPI: API для модели", "https://fastapi.tiangolo.com/tutorial/", "Создайте /predict с валидацией входа и объясните обработку ошибок модели.", 35},
	{"sql", "SQL: запросы и соединения", "https://www.postgresql.org/docs/current/tutorial-sql.html", "Напишите SELECT с JOIN и GROUP BY; объясните отличие INNER JOIN от LEFT JOIN.", 30},
	{"algorithms", "Алгоритмы и структуры данных", "https://ocw.mit.edu/courses/6-006-introduction-to-algorithms-spring-2020/", "Начните с вводной лекции; сравните массив, хеш-таблицу и дерево по сложности операций.", 40},
	{"git", "Git: базовый рабочий процесс", "https://git-scm.com/book/en/v2", "Прочитайте начало Git Basics; создайте ветку, коммит и объясните merge conflict.", 25},
}

// Outline выбирает учебные темы и материалы, сохраняя общие квоты с практикой.
func (e *Engine) Outline(ctx context.Context, in OutlineRequest, actor string) (Outline, error) {
	out := Outline{AIStatus: "UNAVAILABLE", TopicIDs: []string{}, Materials: []Material{}}
	in.Goal = strings.TrimSpace(in.Goal)
	if in.Goal == "" || !utf8.ValidString(in.Goal) || utf8.RuneCountInString(in.Goal) > 1000 || len(in.Topics) > 200 {
		return out, planner.ErrInvalidInput
	}
	seen := map[string]bool{}
	done := map[string]bool{}
	if len(in.CompletedMaterialIDs) > 40 {
		return out, planner.ErrInvalidInput
	}
	for _, id := range in.CompletedMaterialIDs {
		done[id] = true
	}
	type candidate struct {
		Completed bool   `json:"completed"`
		ID        int    `json:"id"`
		Kind      string `json:"kind"`
		Title     string `json:"title"`
	}
	candidates := []candidate{}
	for i, t := range in.Topics {
		if !planner.ValidID(t.ID) || seen[t.ID] || !utf8.ValidString(t.Title) || len(t.Title) > 2000 {
			return out, planner.ErrInvalidInput
		}
		seen[t.ID] = true
		candidates = append(candidates, candidate{false, i + 1, "practice_topic", short(t.Title, 100)})
	}
	for i, m := range library {
		candidates = append(candidates, candidate{done[m.ID], len(in.Topics) + i + 1, "learning_material", m.Title})
	}
	data, _ := json.Marshal(struct {
		Mode       string      `json:"mode"`
		Goal       string      `json:"goal"`
		Candidates []candidate `json:"candidates"`
	}{"select_learning_outline", in.Goal, candidates})
	if len(e.providers) == 0 {
		return out, nil
	}
	snapshot, _ := json.Marshal(in)
	key := sha256.Sum256(append([]byte("outline-v1\x00"+actor+"\x00"), snapshot...))
	validate := func(ids []int) bool {
		if ids == nil || len(ids) > 13 {
			return false
		}
		seen := map[int]bool{}
		topics, materials := 0, 0
		for _, id := range ids {
			if id < 1 || id > len(candidates) || seen[id] || (id > len(in.Topics) && done[library[id-len(in.Topics)-1].ID]) {
				return false
			}
			seen[id] = true
			if id <= len(in.Topics) {
				topics++
			} else {
				materials++
			}
		}
		return topics <= 5 && materials <= 8
	}
	order, status := e.cachedOrder(ctx, key, actor, data, validate)
	out.AIStatus = status
	for _, id := range order {
		if id <= len(in.Topics) {
			out.TopicIDs = append(out.TopicIDs, in.Topics[id-1].ID)
		} else {
			out.Materials = append(out.Materials, library[id-len(in.Topics)-1])
		}
	}
	if (status == "GENERATED" || status == "CACHED") && len(order) == 0 {
		out.AIStatus = "NO_MATCH"
	}
	return out, nil
}
