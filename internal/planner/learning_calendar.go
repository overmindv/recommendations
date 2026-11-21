package planner

import (
	"time"
	_ "time/tzdata"
)

const (
	MaxLearningWeeks    = 26
	maxStudyWeekMinutes = 7 * 24 * 60
)

type StudyBudget struct {
	Timezone         string
	Deadline         string
	AvailableMinutes int
	GoalMinutes      int
	ReservedMinutes  int
	MaxWeeks         int
}

type StudyWeek struct {
	From            string `json:"from"`
	Through         string `json:"through"`
	CapacityMinutes int    `json:"capacity_minutes"`
}

type StudyCalendar struct {
	Timezone       string      `json:"timezone"`
	WeeklyMinutes  int         `json:"weekly_minutes"`
	HorizonLimited bool        `json:"horizon_limited"`
	Weeks          []StudyWeek `json:"weeks"`
	location       *time.Location
}

func StudyWeeks(now time.Time, budget StudyBudget) (StudyCalendar, error) {
	if now.IsZero() || len(budget.Timezone) == 0 || len(budget.Timezone) > 64 || budget.Timezone == "Local" ||
		budget.MaxWeeks < 1 || budget.MaxWeeks > MaxLearningWeeks ||
		!validWeekMinutes(budget.AvailableMinutes) || !validWeekMinutes(budget.GoalMinutes) || !validWeekMinutes(budget.ReservedMinutes) {
		return StudyCalendar{}, ErrInvalidInput
	}
	location, err := time.LoadLocation(budget.Timezone)
	if err != nil {
		return StudyCalendar{}, ErrInvalidInput
	}
	year, month, day := now.In(location).Date()
	first := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	monday := first.AddDate(0, 0, -weekdayIndex(first))
	last := monday.AddDate(0, 0, budget.MaxWeeks*7-1)
	result := StudyCalendar{
		Timezone:      budget.Timezone,
		WeeklyMinutes: min(budget.GoalMinutes, max(0, budget.AvailableMinutes-budget.ReservedMinutes)),
		Weeks:         make([]StudyWeek, 0, budget.MaxWeeks),
		location:      location,
	}
	result.HorizonLimited = true
	if budget.Deadline != "" {
		deadline, err := time.Parse(time.DateOnly, budget.Deadline)
		if err != nil {
			return StudyCalendar{}, ErrInvalidInput
		}
		if !deadline.After(last) {
			result.HorizonLimited = false
			last = deadline
		}
	}
	for week := monday; !week.After(last); week = week.AddDate(0, 0, 7) {
		from := week
		if from.Before(first) {
			from = first
		}
		through := week.AddDate(0, 0, 6)
		if through.After(last) {
			through = last
		}
		if through.Before(from) {
			continue
		}
		minutes := 0
		for date := from; !date.After(through); date = date.AddDate(0, 0, 1) {
			start := localMidnight(date, location)
			if start.Before(now) {
				start = now
			}
			end := localMidnight(date.AddDate(0, 0, 1), location)
			actual := max(0, int(end.Sub(start)/time.Minute))
			weekday := weekdayIndex(date)
			free := min(actual, dailyShare(budget.AvailableMinutes, weekday)) - dailyShare(budget.ReservedMinutes, weekday)
			minutes += min(dailyShare(result.WeeklyMinutes, weekday), max(0, free))
		}
		result.Weeks = append(result.Weeks, StudyWeek{
			From: from.Format(time.DateOnly), Through: through.Format(time.DateOnly), CapacityMinutes: minutes,
		})
	}

	return result, nil
}

func dailyShare(weekly, weekday int) int {
	minutes := weekly / 7
	if weekday < weekly%7 {
		minutes++
	}

	return minutes
}

func validWeekMinutes(minutes int) bool {
	return minutes >= 0 && minutes <= maxStudyWeekMinutes
}

func weekdayIndex(date time.Time) int {
	return (int(date.Weekday()) + 6) % 7
}

func localMidnight(date time.Time, location *time.Location) time.Time {
	year, month, day := date.Date()
	at := time.Date(year, month, day, 0, 0, 0, 0, location)
	y, m, d := at.Date()
	start, end := at.ZoneBounds()
	if time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Before(date) && !end.IsZero() {
		return end
	}
	if !start.IsZero() {
		_, previous := start.Add(-time.Nanosecond).Zone()
		_, current := at.Zone()
		if previous > current {
			earlier := at.Add(time.Duration(current-previous) * time.Second)
			ey, em, ed := earlier.Date()
			hour, minute, second := earlier.Clock()
			if ey == year && em == month && ed == day && hour == 0 && minute == 0 && second == 0 {
				return earlier
			}
		}
	}

	return at
}
