package planner

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestStudyWeeksSeveralGoalsAndDeadline(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	in := StudyBudget{
		Timezone: "UTC", Deadline: "2026-11-24", AvailableMinutes: 140, GoalMinutes: 100,
		ReservedMinutes: 70, MaxWeeks: 3,
	}
	got, err := StudyWeeks(now, in)
	want := []StudyWeek{
		{From: "2026-11-18", Through: "2026-11-22", CapacityMinutes: 50},
		{From: "2026-11-23", Through: "2026-11-24", CapacityMinutes: 20},
	}
	if err != nil || got.WeeklyMinutes != 70 || got.HorizonLimited || !slices.Equal(got.Weeks, want) {
		t.Fatal("unexpected allocation around the deadline:", got, err)
	}
	in.ReservedMinutes = 200
	empty, err := StudyWeeks(now, in)
	if err != nil || empty.WeeklyMinutes != 0 || empty.Weeks[0].CapacityMinutes != 0 {
		t.Fatal("oversubscribed calendar gave the goal time:", empty, err)
	}
}

func TestStudyWeeksReservesTheLastMinuteForExistingGoals(t *testing.T) {
	now := time.Date(2026, 11, 16, 23, 59, 0, 0, time.UTC)
	in := StudyBudget{
		Timezone: "UTC", Deadline: "2026-11-16", AvailableMinutes: 14, GoalMinutes: 7, ReservedMinutes: 7, MaxWeeks: 1,
	}
	got, err := StudyWeeks(now, in)
	if err != nil || len(got.Weeks) != 1 || got.Weeks[0].CapacityMinutes != 0 {
		t.Fatal("ended up allocating the last minute to two goals:", got, err)
	}
}

func TestStudyWeeksUsesLocalDatesAcrossDST(t *testing.T) {
	in := StudyBudget{
		Timezone: "America/New_York", Deadline: "2026-03-09", AvailableMinutes: maxStudyWeekMinutes,
		GoalMinutes: maxStudyWeekMinutes, MaxWeeks: 2,
	}
	now := time.Date(2026, 3, 8, 4, 30, 0, 0, time.UTC)
	got, err := StudyWeeks(now, in)
	want := []StudyWeek{
		{From: "2026-03-07", Through: "2026-03-08", CapacityMinutes: 30 + 23*60},
		{From: "2026-03-09", Through: "2026-03-09", CapacityMinutes: 24 * 60},
	}
	if err != nil || !slices.Equal(got.Weeks, want) {
		t.Fatal("calendar assumed every day has 24 hours or used the UTC date:", got, err)
	}
}

func TestStudyWeeksWithSkippedMidnight(t *testing.T) {
	in := StudyBudget{
		Timezone: "America/Santiago", Deadline: "2026-09-06", AvailableMinutes: maxStudyWeekMinutes,
		GoalMinutes: maxStudyWeekMinutes, MaxWeeks: 1,
	}
	now := time.Date(2026, 9, 6, 3, 30, 0, 0, time.UTC)
	got, err := StudyWeeks(now, in)
	want := []StudyWeek{{From: "2026-09-05", Through: "2026-09-06", CapacityMinutes: 30 + 23*60}}
	if err != nil || !slices.Equal(got.Weeks, want) {
		t.Fatal("skipped midnight changed the local deadline budget:", got, err)
	}
}

func TestStudyWeeksLimitsTheHorizon(t *testing.T) {
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	in := StudyBudget{Timezone: "UTC", AvailableMinutes: 61, GoalMinutes: 61, MaxWeeks: 1, Deadline: "2026-12-31"}
	got, err := StudyWeeks(now, in)
	if err != nil || !got.HorizonLimited || len(got.Weeks) != 1 || got.Weeks[0].Through != "2026-11-22" ||
		got.Weeks[0].CapacityMinutes != 61 {
		t.Fatal("calendar exceeded its horizon or lost a remainder:", got, err)
	}
	in.Deadline = "2026-11-15"
	past, err := StudyWeeks(now, in)
	if err != nil || past.HorizonLimited || len(past.Weeks) != 0 {
		t.Fatal("calendar allocated time after a past deadline:", past, err)
	}
	in.Deadline = "2026-02-30"
	if _, err := StudyWeeks(now, in); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("invalid date accepted:", err)
	}
}
