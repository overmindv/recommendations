package httpapi

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/overmindv/recommendations/internal/studyactivity"
)

const activityPath = "/v1/learner/study-activity"

type activityFake struct{ err error }

func (f activityFake) Capture(_ context.Context, id string, in studyactivity.Input) (studyactivity.Receipt, error) {
	if id != actor {
		return studyactivity.Receipt{}, errors.New("wrong actor")
	}

	return studyactivity.Receipt{ClientEventID: in.ClientEventID, Trust: studyactivity.Trust}, f.err
}

func TestStudyActivityHTTP(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err    error
		status int
	}{
		{nil, 200}, {studyactivity.ErrInvalid, 400}, {studyactivity.ErrConflict, 409},
		{studyactivity.ErrRate, 429}, {studyactivity.ErrResource, 422}, {errors.New("private database detail"), 503},
	} {
		mux := http.NewServeMux()
		RegisterStudyActivity(mux, token, activityFake{err: tc.err})
		r := onboardingRequest(http.MethodPost, activityPath, actor, `{}`, 0, "")
		performOnboarding(t, mux, r, tc.status, "")
	}
	mux := http.NewServeMux()
	RegisterStudyActivity(mux, token, activityFake{})
	for _, body := range []string{`{"actor":"spoof"}`, `{"code":"secret"}`, `{} {}`, `{"duration_seconds":1.1}`} {
		performOnboarding(t, mux, onboardingRequest(http.MethodPost, activityPath, actor, body, 0, ""), 400, codeInvalidArgument)
	}
	r := onboardingRequest(http.MethodPost, activityPath, actor, `{}`, 0, "")
	r.Header.Add("X-User-ID", actor)
	performOnboarding(t, mux, r, 403, "PERMISSION_DENIED")
	r = onboardingRequest(http.MethodPost, activityPath, actor, `{}`, 0, "")
	r.Header.Del("Authorization")
	performOnboarding(t, mux, r, 403, "PERMISSION_DENIED")
	disabled := http.NewServeMux()
	RegisterStudyActivity(disabled, token, nil)
	performOnboarding(t, disabled, onboardingRequest(http.MethodPost, activityPath, actor, `{}`, 0, ""), 503, "SERVICE_UNAVAILABLE")
}
