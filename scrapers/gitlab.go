package scrapers

import (
	"SSH-Key-Scraper/scrapers/gitlab"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Khan/genqlient/graphql"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// concurrentRequests is the number of concurrent requests to make to the GitLab REST API
const concurrentRequests = 10

type GitLabScraper struct {
	*Scraper
}

type GitLabPublicKeyEntry struct {
	DatabaseID int64     `json:"databaseId"`
	Key        string    `json:"key"`
	Title      string    `json:"title"`
	UsageType  string    `json:"usageType"`
	CreatedAt  time.Time `json:"createdAt"`
	ExpiresAt  time.Time `json:"expiresAt"`
	VisitedAt  time.Time `json:"visitedAt"`
	Deleted    bool      `json:"deleted"`
}

type GitLabUserEntry struct {
	DatabaseID     string                 `json:"databaseId"`
	Username       string                 `json:"username"`
	State          string                 `json:"state"`
	CreatedAt      time.Time              `json:"createdAt"`
	LastActivityOn time.Time              `json:"lastActivityOn"`
	VisitedAt      time.Time              `json:"visitedAt"`
	Deleted        bool                   `json:"deleted"`
	PublicKeys     []GitLabPublicKeyEntry `json:"publicKeys"`
}

type gitlabTransport struct {
	token   string
	graphql bool
	wrapped http.RoundTripper
}

func (t *gitlabTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.graphql {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.token))
	} else {
		req.Header.Set("PRIVATE-TOKEN", t.token)
	}
	return t.wrapped.RoundTrip(req)
}

func (s *GitLabScraper) newGraphQLClient() graphql.Client {
	httpClient := http.Client{
		Transport: &gitlabTransport{
			token:   s.getPlatformConfigString("token"),
			graphql: true,
			wrapped: http.DefaultTransport,
		},
	}
	return graphql.NewClient("https://gitlab.com/api/graphql", &httpClient)
}

func (s *GitLabScraper) newRestClient() *http.Client {
	return &http.Client{
		Transport: &gitlabTransport{
			token:   s.getPlatformConfigString("token"),
			graphql: false,
			wrapped: http.DefaultTransport,
		},
	}
}

func (s *GitLabScraper) updateCursor(ctx context.Context, last *gitlab.GetUsersUsersUserCoreConnectionNodesUserCore) {
	if last == nil {
		return
	}
	type cursor struct {
		CreatedAt string `json:"created_at"`
		Id        string `json:"id"`
	}
	idParts := strings.Split(last.Id, "/")
	cursorData := cursor{
		CreatedAt: last.CreatedAt.Format("2006-01-02T15:04:05Z"),
		Id:        idParts[len(idParts)-1],
	}
	cursorJson, _ := json.Marshal(cursorData)
	s.Cursor = base64.StdEncoding.EncodeToString(cursorJson)
	if err := s.Save(ctx); err != nil {
		s.log("failed to save cursor: %v", true, err)
	}
	s.log("cursor updated, new cursor: %v", false, s.Cursor)
}

func (s *GitLabScraper) mapToUserEntry(user *gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, publicKeys *[]gitlab.GitLabPublicKey, existing *GitLabUserEntry) *GitLabUserEntry {
	now := time.Now()
	entry := &GitLabUserEntry{
		DatabaseID:     user.Id,
		Username:       user.Username,
		State:          string(user.State),
		CreatedAt:      user.CreatedAt,
		LastActivityOn: user.LastActivityOn,
		VisitedAt:      now,
		Deleted:        false,
	}
	if existing != nil {
		entry.PublicKeys = existing.PublicKeys
	} else {
		entry.PublicKeys = []GitLabPublicKeyEntry{}
	}
	for _, key := range *publicKeys {
		exists := false
		for _, existingKey := range entry.PublicKeys {
			if existingKey.Key == key.Key {
				exists = true
				existingKey.Deleted = false
				existingKey.VisitedAt = now
			}
		}
		if !exists {
			entry.PublicKeys = append(entry.PublicKeys, GitLabPublicKeyEntry{
				DatabaseID: key.Id,
				Key:        key.Key,
				Title:      key.Title,
				UsageType:  key.UsageType,
				CreatedAt:  key.CreatedAt,
				ExpiresAt:  key.ExpiresAt,
				VisitedAt:  now,
				Deleted:    false,
			})
		}
	}
	return entry
}

func (s *GitLabScraper) createUserIndex(ctx context.Context) error {
	// Check if user index exists, create it if it doesn't
	exists, err := s.Elasticsearch.Indices.Exists(s.UserIndex).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if user index exists: %w", err)
	}
	if exists {
		return nil
	}
	_, err = s.Elasticsearch.Indices.
		Create(s.UserIndex).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"databaseId":     types.NewKeywordProperty(),
					"username":       types.NewKeywordProperty(),
					"state":          types.NewKeywordProperty(),
					"createdAt":      types.NewDateProperty(),
					"lastActivityOn": types.NewDateProperty(),
					"publicKeys": &types.NestedProperty{
						Properties: map[string]types.Property{
							"databaseId": types.NewLongNumberProperty(),
							"key":        types.NewTextProperty(),
							"title":      types.NewTextProperty(),
							"usageType":  types.NewKeywordProperty(),
							"createdAt":  types.NewDateProperty(),
							"expiresAt":  types.NewDateProperty(),

							// Fields added by the scraper
							"visitedAt": types.NewDateProperty(),
							"deleted":   types.NewBooleanProperty(),
						},
					},

					// Fields added by the scraper
					"visitedAt": types.NewDateProperty(),
					"deleted":   types.NewBooleanProperty(),
				},
			},
			Settings: &types.IndexSettings{
				NumberOfShards:   "1",
				NumberOfReplicas: "2",
			},
		}).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to create user index: %w", err)
	}
	return nil
}

func (s *GitLabScraper) processResponse(ctx context.Context, user *gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, publicKeys []gitlab.GitLabPublicKey) {
	searchResult, err := s.Elasticsearch.Search().
		Index(s.UserIndex).
		Request(&search.Request{
			Query: &types.Query{
				Match: map[string]types.MatchQuery{
					"databaseId": {Query: user.Id},
				},
			},
		}).Do(ctx)
	if err != nil {
		// If anything goes wrong, we save the unprocessed user to a file
		err := s.saveUnprocessedUser(user.Username, user)
		if err != nil {
			panic(err)
		}
		return
	}
	var entry *GitLabUserEntry
	if searchResult.Hits.Total.Value == 0 {
		entry = s.mapToUserEntry(user, &publicKeys, nil)
		_, err = s.Elasticsearch.Index(s.UserIndex).
			Request(entry).
			Do(ctx)
		if err != nil {
			err := s.saveUnprocessedUser(user.Username, user)
			if err != nil {
				panic(err)
			}
			return
		}
	} else {
		var existing *GitLabUserEntry
		if err = json.Unmarshal(searchResult.Hits.Hits[0].Source_, &existing); err != nil {
			err := s.saveUnprocessedUser(user.Username, user)
			if err != nil {
				panic(err)
			}
			return
		}
		entry = s.mapToUserEntry(user, &publicKeys, entry)
		_, err = s.Elasticsearch.Index(s.UserIndex).
			Id(*searchResult.Hits.Hits[0].Id_).
			Request(entry).
			Do(ctx)
		if err != nil {
			err := s.saveUnprocessedUser(user.Username, user)
			if err != nil {
				panic(err)
			}
			return
		}
	}
}

func (s *GitLabScraper) publicKeyWorker(ctx context.Context, users <-chan gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, failures chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	restClient := s.newRestClient()
	for user := range users {
		res, err := restClient.Get(fmt.Sprintf("https://gitlab.com/api/v4/users/%v/keys", url.QueryEscape(user.Username)))
		if err != nil {
			// If we encounter any error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("apiErrorCooldown"))
			failures <- err
			return
		}
		if res.StatusCode != 200 {
			if res.StatusCode == 429 {
				// If we hit the rate limit, we wait for the rate limit reset time before continuing
				if resetTime, err := time.Parse(time.RFC1123, res.Header.Get("RateLimit-ResetTime")); err == nil {
					s.ContinueAt = resetTime.Add(1 * time.Minute)
					s.log("hit rate limit, continuing at %v", false, s.ContinueAt)
				} else {
					s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("apiErrorCooldown"))
					s.log("hit rate limit but failed to parse rate limit reset time, continuing at %v", true, s.ContinueAt)
				}
				failures <- fmt.Errorf("rate limit hit")
				return
			} else if res.StatusCode == 404 {
				// If the API cannot find a user by the given username, we skip the user
				s.log("user %v not found, continuing", false, user.Username)
				continue
			}
			// If we encounter any unknown error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("apiErrorCooldown"))
			failures <- fmt.Errorf("unexpected status code while retrieving public keys for user %v: %v", user.Username, res.StatusCode)
			return
		}
		if res.Header.Get("Content-Type") != "application/json" {
			failures <- fmt.Errorf("unexpected content type: %v", res.Header.Get("Content-Type"))
			return
		}
		var publicKeys []gitlab.GitLabPublicKey
		if err := json.NewDecoder(res.Body).Decode(&publicKeys); err != nil {
			// If we encounter any error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("apiErrorCooldown"))
			failures <- fmt.Errorf("failed to decode public keys from json for user %v: %w", user.Username, err)
			return
		}
		s.processResponse(ctx, &user, publicKeys)
	}
}

func (s *GitLabScraper) Scrape(ctx context.Context) (bool, error) {
	if err := s.createUserIndex(ctx); err != nil {
		panic(err)
	}
	gqlClient := s.newGraphQLClient()
	if s.Cursor == "" {
		s.Cursor = s.getPlatformConfigString("initialCursor")
		if err := s.Save(ctx); err != nil {
			panic(err)
		}
	}

	s.log("starting scraping from %v", false, s.Cursor)

	wg := sync.WaitGroup{}
	var res *gitlab.GetUsersResponse
	var err error
	for {
		iterationStart := time.Now()
		// Start by fetching the next page of users
		res, err = gitlab.GetUsers(ctx, gqlClient, s.Cursor)
		if err != nil {
			// If we encounter any error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("apiErrorCooldown"))
			return false, err
		}

		users := make(chan gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, len(res.Users.Nodes))
		failures := make(chan error, concurrentRequests)

		wg.Add(concurrentRequests)
		for i := 0; i < concurrentRequests; i++ {
			go s.publicKeyWorker(ctx, users, failures, &wg)
		}

		for _, user := range res.Users.Nodes {
			users <- user
		}
		close(users)
		// Wait for processing to complete
		wg.Wait()

		if len(failures) > 0 {
			// If we encounter any error, we wait for the configured duration before continuing
			return false, <-failures
		}

		// Update the cursor to the last user we have seen
		s.updateCursor(ctx, &res.Users.Nodes[len(res.Users.Nodes)-1])
		if !res.Users.PageInfo.HasNextPage {
			return true, nil
		}
		iterationEnd := time.Now()
		iterationDuration := iterationEnd.Sub(iterationStart)
		if iterationDuration < s.MinimumIterationDuration {
			time.Sleep(s.MinimumIterationDuration - iterationDuration)
		}
	}
}
