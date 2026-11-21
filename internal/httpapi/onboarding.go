package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/topiccatalog"
)

const (
	maxOnboardingBody       = 8 << 10
	feasibilityNotEvaluated = "not_evaluated"
	feasibilityInfeasible   = "infeasible"
	reasonDeadlinePassed    = "deadline_passed"
)

type onboardingHandler struct {
	store learnergoals.Store
	now   func() time.Time
}

func RegisterOnboarding(router Router, token string, store learnergoals.Store) {
	handler := onboardingHandler{store: store, now: time.Now}
	expected := sha256.Sum256([]byte(token))
	handle := func(route string, requiresStore bool, fn http.HandlerFunc) {
		endpoint := http.TimeoutHandler(fn, 4*time.Second, `{"code":"SERVICE_UNAVAILABLE","message":"Service unavailable"}`)
		router.Handle(route, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", noStoreCache)
			w.Header().Set("Content-Type", jsonContentType)
			if !authorized(r, token, expected) {
				writeJSON(w, http.StatusForbidden, publicError{"PERMISSION_DENIED", "Permission denied"})

				return
			}
			if requiresStore && store == nil {
				writeJSON(w, http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"})

				return
			}
			endpoint.ServeHTTP(w, r)
		}))
	}
	handle("GET /v1/learner/goal-templates", false, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, learnergoals.Templates())
	})
	handle("GET /v1/learner/profile", true, handler.getProfile)
	handle("PUT /v1/learner/profile", true, handler.saveProfile)
	handle("GET /v1/learner/goals", true, handler.listGoals)
	handle("POST /v1/learner/goals", true, handler.createGoal)
	handle("GET /v1/learner/goals/{id}", true, handler.getGoal)
	handle("PUT /v1/learner/goals/{id}", true, handler.updateGoal)
	handle("DELETE /v1/learner/goals/{id}", true, handler.deleteGoal)
}

type profileView struct {
	Availability *learnergoals.Availability `json:"availability"`
}

type topicSkill struct {
	TopicID string `json:"topic_id"`
	State   string `json:"state"`
}

type goalView struct {
	learnergoals.Goal
	Skills            []topicSkill `json:"skills"`
	Feasibility       string       `json:"feasibility"`
	FeasibilityReason string       `json:"feasibility_reason,omitempty"`
}

func presentGoal(goal learnergoals.Goal, now time.Time) goalView {
	view := goalView{Goal: goal, Skills: make([]topicSkill, 0, len(goal.TargetTopics)), Feasibility: feasibilityNotEvaluated}
	for _, id := range goal.TargetTopics {
		view.Skills = append(view.Skills, topicSkill{TopicID: id, State: "unknown"})
	}
	if goal.DeadlinePassed(now) {
		view.Feasibility = feasibilityInfeasible
		view.FeasibilityReason = reasonDeadlinePassed
	}

	return view
}

func (h onboardingHandler) getProfile(w http.ResponseWriter, r *http.Request) {
	result, err := h.store.GetAvailability(r.Context(), r.Header.Get("X-User-ID"))
	if errors.Is(err, learnergoals.ErrNotFound) {
		writeJSON(w, http.StatusOK, profileView{})
	} else if err != nil {
		writeOnboardingError(w, err)
	} else {
		writeJSON(w, http.StatusOK, profileView{Availability: &result})
	}
}

func (h onboardingHandler) saveProfile(w http.ResponseWriter, r *http.Request) {
	write, ok := onboardingWrite(w, r, 0)
	var input learnergoals.AvailabilityInput
	if !ok || !decodeOnboarding(w, r, &input) {
		return
	}
	result, err := h.store.SaveAvailability(r.Context(), r.Header.Get("X-User-ID"), write, input)
	if err != nil {
		writeOnboardingError(w, err)

		return
	}
	writeJSON(w, http.StatusOK, profileView{Availability: &result})
}

func (h onboardingHandler) listGoals(w http.ResponseWriter, r *http.Request) {
	goals, err := h.store.List(r.Context(), r.Header.Get("X-User-ID"))
	if err != nil {
		writeOnboardingError(w, err)

		return
	}
	results := make([]goalView, 0, len(goals))
	now := h.now()
	for _, goal := range goals {
		results = append(results, presentGoal(goal, now))
	}
	writeJSON(w, http.StatusOK, results)
}

func (h onboardingHandler) getGoal(w http.ResponseWriter, r *http.Request) {
	goal, err := h.store.Get(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"))
	respondGoal(w, http.StatusOK, goal, h.now(), err)
}

func (h onboardingHandler) createGoal(w http.ResponseWriter, r *http.Request) {
	write, ok := onboardingWrite(w, r, -1)
	var input learnergoals.Input
	if !ok || !decodeOnboarding(w, r, &input) {
		return
	}
	goal, err := h.store.Create(r.Context(), r.Header.Get("X-User-ID"), write.RequestID, input)
	respondGoal(w, http.StatusCreated, goal, goal.UpdatedAt, err)
}

func (h onboardingHandler) updateGoal(w http.ResponseWriter, r *http.Request) {
	write, ok := onboardingWrite(w, r, 1)
	var input learnergoals.Input
	if !ok || !decodeOnboarding(w, r, &input) {
		return
	}
	goal, err := h.store.Update(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), write, input)
	respondGoal(w, http.StatusOK, goal, goal.UpdatedAt, err)
}

func (h onboardingHandler) deleteGoal(w http.ResponseWriter, r *http.Request) {
	write, ok := onboardingWrite(w, r, 1)
	if !ok {
		return
	}
	if err := h.store.Delete(r.Context(), r.Header.Get("X-User-ID"), r.PathValue("id"), write); err != nil {
		writeOnboardingError(w, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func respondGoal(w http.ResponseWriter, status int, goal learnergoals.Goal, at time.Time, err error) {
	if err != nil {
		writeOnboardingError(w, err)

		return
	}
	writeJSON(w, status, presentGoal(goal, at))
}

func onboardingWrite(w http.ResponseWriter, r *http.Request, minVersion int64) (learnergoals.Write, bool) {
	keys := r.Header.Values("Idempotency-Key")
	versions := r.Header.Values("X-Expected-Version")
	if len(keys) != 1 || !planner.ValidID(keys[0]) {
		writeOnboardingError(w, learnergoals.ErrInvalid)

		return learnergoals.Write{}, false
	}
	result := learnergoals.Write{RequestID: keys[0]}
	if minVersion == -1 && len(versions) == 0 {
		return result, true
	}
	if minVersion == -1 || len(versions) != 1 {
		writeOnboardingError(w, learnergoals.ErrInvalid)

		return learnergoals.Write{}, false
	}
	version, err := strconv.ParseInt(versions[0], 10, 64)
	if err != nil || version < minVersion || strconv.FormatInt(version, 10) != versions[0] {
		writeOnboardingError(w, learnergoals.ErrInvalid)

		return learnergoals.Write{}, false
	}
	result.ExpectedVersion = version

	return result, true
}

func decodeOnboarding(w http.ResponseWriter, r *http.Request, value any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != jsonContentType || len(r.Header.Values("Content-Type")) != 1 {
		writeJSON(w, http.StatusUnsupportedMediaType, publicError{codeInvalidArgument, "Expected application/json"})

		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxOnboardingBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(value)
	if err == nil {
		var extra any
		err = decoder.Decode(&extra)
		if errors.Is(err, io.EOF) {
			return true
		}
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, publicError{codeInvalidArgument, "Request too large"})
	} else {
		writeOnboardingError(w, learnergoals.ErrInvalid)
	}

	return false
}

func writeOnboardingError(w http.ResponseWriter, err error) {
	var status int
	var response publicError
	switch {
	case errors.Is(err, learnergoals.ErrDeadline):
		status, response = http.StatusUnprocessableEntity, publicError{"INFEASIBLE_GOAL", "Goal deadline has passed"}
	case errors.Is(err, learnergoals.ErrBudget):
		status, response = http.StatusUnprocessableEntity, publicError{"INFEASIBLE_GOAL", "Goals exceed the weekly budget"}
	case errors.Is(err, topiccatalog.ErrTopicUnavailable):
		status, response = http.StatusUnprocessableEntity, publicError{"UNKNOWN_TOPIC", "A target topic is unavailable"}
	case errors.Is(err, learnergoals.ErrUnknownTemplate):
		status, response = http.StatusUnprocessableEntity, publicError{"UNKNOWN_TEMPLATE", "Goal template is unavailable"}
	case errors.Is(err, learnergoals.ErrInvalid):
		status, response = http.StatusBadRequest, publicError{codeInvalidArgument, "Invalid onboarding request"}
	case errors.Is(err, learnergoals.ErrNoAvailability):
		status, response = http.StatusConflict, publicError{"PROFILE_REQUIRED", "Set availability before creating a goal"}
	case errors.Is(err, learnergoals.ErrNotFound):
		status, response = http.StatusNotFound, publicError{"NOT_FOUND", "Not found"}
	case errors.Is(err, learnergoals.ErrConflict):
		status, response = http.StatusConflict, publicError{"VERSION_CONFLICT", "Version changed"}
	case errors.Is(err, learnergoals.ErrRequestUsed):
		status, response = http.StatusConflict, publicError{"IDEMPOTENCY_CONFLICT", "Key already used"}
	case errors.Is(err, learnergoals.ErrGoalLimit):
		status, response = http.StatusConflict, publicError{"GOAL_LIMIT", "Goal limit reached"}
	case errors.Is(err, learnergoals.ErrRequestLimit):
		status, response = http.StatusTooManyRequests, publicError{"WRITE_LIMIT", "Write limit reached"}
	default:
		status, response = http.StatusServiceUnavailable, publicError{"SERVICE_UNAVAILABLE", "Service unavailable"}
	}
	writeJSON(w, status, response)
}
