package worker

import "strings"

func normalizeQueueName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return DefaultQueueName
	}

	return name
}

func normalizeQueueWeight(weight int) int {
	if weight <= 0 {
		return DefaultQueueWeight
	}

	return weight
}

func copyQueueWeights(weights map[string]int) map[string]int {
	out := make(map[string]int, len(weights))
	for name, weight := range weights {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		out[name] = normalizeQueueWeight(weight)
	}

	return out
}
