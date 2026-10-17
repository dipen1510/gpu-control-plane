package metrics

import (
	dto "github.com/prometheus/client_model/go"
)

// count returns the value of the series of family name whose only label
// value is labelValue ("" for a series with no labels), or -1 if absent.
func count(families []*dto.MetricFamily, name, labelValue string) float64 {
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			var v string
			for _, l := range m.GetLabel() {
				v = l.GetValue()
			}
			if v != labelValue {
				continue
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}
