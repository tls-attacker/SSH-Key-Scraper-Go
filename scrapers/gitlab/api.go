package gitlab

import "time"

//go:generate go run github.com/Khan/genqlient

type GitLabPublicKey struct {
	Id        int64     `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Key       string    `json:"key"`
	UsageType string    `json:"usage_type"`
}

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
