package planner

func (a *studyAllocator) preserve(fresh StudyCalendar, weeks []LearningWeek) error {
	if len(weeks) > MaxLearningWeeks {
		return ErrInvalidInput
	}
	a.weeks = make([]LearningWeek, len(weeks))
	a.carried = make(map[string]map[StudyStage]int)
	var previous string
	current := 0
	for i, week := range weeks {
		if week.From <= previous || week.Through < week.From ||
			week.CapacityMinutes < 0 || week.CapacityMinutes > fresh.WeeklyMinutes {
			return ErrInvalidInput
		}
		previous = week.Through
		for current < len(fresh.Weeks) && fresh.Weeks[current].Through < week.From {
			current++
		}
		capacity := 0
		if current < len(fresh.Weeks) && fresh.Weeks[current].Through == week.Through {
			capacity = min(week.CapacityMinutes, fresh.Weeks[current].CapacityMinutes)
		}
		a.weeks[i] = LearningWeek{StudyWeek: week.StudyWeek, Items: []StudyItem{}}
		for _, item := range week.Items {
			if item.Minutes < 1 || item.Minutes > week.CapacityMinutes-a.weeks[i].PlannedMinutes ||
				a.count == MaxLearningItems || a.used[studyResourceID{item.Kind, item.ID}] {
				return ErrInvalidInput
			}
			item.Decision = nil
			a.place(i, item)
			a.resume = i
			if !item.Superseded {
				if a.carried[item.TopicID] == nil {
					a.carried[item.TopicID] = make(map[StudyStage]int)
				}
				a.carried[item.TopicID][item.Stage] = i
			}
		}
		a.weeks[i].CapacityMinutes = max(capacity, a.weeks[i].PlannedMinutes)
	}

	return nil
}
