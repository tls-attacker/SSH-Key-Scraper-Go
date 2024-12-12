package gitlab

import "time"

//go:generate go run github.com/Khan/genqlient

func MarshalDate(v *time.Time) ([]byte, error) {
	return []byte(v.Format("\"2006-01-02\"")), nil
}

func UnmarshalDate(b []byte, v *time.Time) error {
	if t, err := time.Parse("\"2006-01-02\"", string(b)); err != nil {
		return err
	} else {
		*v = t
		return nil
	}
}
