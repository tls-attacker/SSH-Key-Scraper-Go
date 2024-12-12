package scrapers

import (
	"SSH-Key-Scraper/scrapers/github"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Khan/genqlient/graphql"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The initialCursor which we use to start scraping from
// This should be less or equal to the registration date of the first user
var initialCursor = time.Date(2007, 10, 20, 00, 00, 00, 00, time.UTC)

// The timespan which we request in a single request from the search API (30 days)
const timespan = 30 * 24 * time.Hour

// The userIndex in which we store the API responses for further processing
const userIndex = "sshks_github_users"

// The maximum number of users GitHub returns to a single search request (even with pagination)
const searchLimit = 1000

type githubTransport struct {
	token   string
	wrapped http.RoundTripper
}

func (t *githubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.token))
	return t.wrapped.RoundTrip(req)
}

func newGraphQLClient() graphql.Client {
	httpClient := http.Client{
		Transport: &githubTransport{
			token:   viper.GetString("github.token"),
			wrapped: http.DefaultTransport,
		},
	}
	return graphql.NewClient("https://api.github.com/graphql", &httpClient)
}

func compileQueryString(cursor string) string {
	startDate, _ := time.Parse(time.RFC3339, cursor)
	endDate := startDate.Add(timespan)
	return fmt.Sprintf("created:%s..%s type:user sort:joined-asc",
		startDate.UTC().Format("2006-01-02T15:04:05Z"),
		endDate.UTC().Format("2006-01-02T15:04:05Z"))
}

func updateCursor(ctx context.Context, s *Scraper, res *github.GetSshPublicKeysResponse) {
	if res.Search.UserCount < searchLimit && !res.Search.PageInfo.HasNextPage {
		current, _ := time.Parse(time.RFC3339, s.Cursor)
		current = current.Add(timespan)
		if current.After(time.Now()) {
			current = time.Now()
		}
		s.Cursor = current.Format(time.RFC3339)
	} else if user, ok := res.Search.Nodes[len(res.Search.Nodes)-1].(*github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser); ok {
		s.Cursor = user.CreatedAt.Format(time.RFC3339)
	}
	if err := s.Save(ctx); err != nil {
		log.Printf("[!][%v] failed to save cursor: %v", s.Platform, err)
	}
	log.Printf("[i][%v] cursor updated, new cursor: %v", s.Platform, s.Cursor)
}

type GitHubPublicKeyEntry struct {
	Key         string    `json:"key"`
	Fingerprint string    `json:"fingerprint"`
	VisitedAt   time.Time `json:"visitedAt"`
	Deleted     bool      `json:"deleted"`
}

type GitHubUserEntry struct {
	DatabaseId string                 `json:"databaseId"`
	Login      string                 `json:"login"`
	CreatedAt  time.Time              `json:"createdAt"`
	UpdatedAt  time.Time              `json:"updatedAt"`
	VisitedAt  time.Time              `json:"visitedAt"`
	Deleted    bool                   `json:"deleted"`
	PublicKeys []GitHubPublicKeyEntry `json:"publicKeys"`
}

func mapToUserEntry(user *github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser, existing *GitHubUserEntry) *GitHubUserEntry {
	now := time.Now()
	entry := &GitHubUserEntry{
		DatabaseId: strconv.Itoa(user.DatabaseId),
		Login:      user.Login,
		CreatedAt:  user.CreatedAt,
		UpdatedAt:  user.UpdatedAt,
		VisitedAt:  now,
		Deleted:    false,
	}
	if existing != nil {
		entry.PublicKeys = existing.PublicKeys
	} else {
		entry.PublicKeys = []GitHubPublicKeyEntry{}
	}
	for _, key := range user.PublicKeys.Nodes {
		exists := false
		for _, existingKey := range entry.PublicKeys {
			if existingKey.Fingerprint == key.Fingerprint {
				exists = true
				existingKey.Deleted = false
				existingKey.VisitedAt = now
			}
		}
		if !exists {
			entry.PublicKeys = append(entry.PublicKeys, GitHubPublicKeyEntry{
				Key:         key.Key,
				Fingerprint: key.Fingerprint,
				VisitedAt:   time.Now(),
				Deleted:     false,
			})
		}
	}
	return entry
}

func createUserIndex(ctx context.Context, s *Scraper) error {
	exists, err := s.Elasticsearch.Indices.Exists(userIndex).Do(ctx)
	if err != nil {
		return fmt.Errorf("[!][%v] failed to check if user index exists: %w", s.Platform, err)
	}
	if exists {
		return nil
	}
	_, err = s.Elasticsearch.Indices.
		Create(userIndex).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"databaseId": types.NewKeywordProperty(),
					"login":      types.NewKeywordProperty(),
					"createdAt":  types.NewDateProperty(),
					"updatedAt":  types.NewDateProperty(),
					"visitedAt":  types.NewDateProperty(),
					"deleted":    types.NewBooleanProperty(),
					"publicKeys": &types.NestedProperty{
						Properties: map[string]types.Property{
							"key":         types.NewTextProperty(),
							"fingerprint": types.NewTextProperty(),
							"visitedAt":   types.NewDateProperty(),
							"deleted":     types.NewBooleanProperty(),
						},
					},
				},
			},
			Settings: &types.IndexSettings{
				NumberOfShards:   "1",
				NumberOfReplicas: "2",
			},
		}).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("[!][%v] failed to create user index: %w", s.Platform, err)
	}
	return nil
}

func saveUnprocessedUser(s *Scraper, user *github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser) error {
	if _, err := os.Stat("unprocessed_github"); os.IsNotExist(err) {
		if err := os.Mkdir("unprocessed_github", 0755); err != nil {
			return fmt.Errorf("[!][%v] failed to create directory for unprocessed user files: %w", s.Platform, err)
		}
	}
	file, err := os.Create(fmt.Sprintf("unprocessed_github/%s.json", user.Login))
	if err != nil {
		return fmt.Errorf("[!][%v] failed to save unprocessed user to file: %w", s.Platform, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(user); err != nil {
		return fmt.Errorf("[!][%v] failed to encode unprocessed user to json: %w", s.Platform, err)
	}
	return nil
}

func processResponse(ctx context.Context, s *Scraper, res github.GetSshPublicKeysResponse, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, user := range res.Search.Nodes {
		if user, ok := user.(*github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser); ok {
			searchResult, err := s.Elasticsearch.Search().
				Index(userIndex).
				Request(&search.Request{
					Query: &types.Query{
						Match: map[string]types.MatchQuery{
							"databaseId": {Query: strconv.Itoa(user.DatabaseId)},
						},
					},
				}).Do(ctx)
			if err != nil {
				// If anything goes wrong, we save the unprocessed user to a file
				err := saveUnprocessedUser(s, user)
				if err != nil {
					panic(err)
				}
				continue
			}
			var entry *GitHubUserEntry
			if searchResult.Hits.Total.Value == 0 {
				entry = mapToUserEntry(user, nil)
				_, err = s.Elasticsearch.Index(userIndex).
					Request(entry).
					Do(ctx)
				if err != nil {
					err := saveUnprocessedUser(s, user)
					if err != nil {
						panic(err)
					}
					continue
				}
			} else {
				var existing *GitHubUserEntry
				if err = json.Unmarshal(searchResult.Hits.Hits[0].Source_, &existing); err != nil {
					err := saveUnprocessedUser(s, user)
					if err != nil {
						panic(err)
					}
					continue
				}
				entry = mapToUserEntry(user, entry)
				_, err = s.Elasticsearch.Index(userIndex).
					Id(*searchResult.Hits.Hits[0].Id_).
					Request(entry).
					Do(ctx)
				if err != nil {
					err := saveUnprocessedUser(s, user)
					if err != nil {
						panic(err)
					}
					continue
				}
			}
		}
	}
}

func ScrapePlatform(ctx context.Context, s *Scraper) (bool, error) {
	if err := createUserIndex(ctx, s); err != nil {
		panic(err)
	}
	client := newGraphQLClient()
	if s.Cursor == "" {
		s.Cursor = initialCursor.Format(time.RFC3339)
		if err := s.Save(ctx); err != nil {
			panic(err)
		}
	}

	log.Printf("[i][%v] starting scraping from %v", s.Platform, s.Cursor)

	wg := sync.WaitGroup{}
	var res *github.GetSshPublicKeysResponse
	var err error
	rateLimitRemaining := 5000
	for {
		iterationStart := time.Now()
		// A single request usually costs between 1-2 rate limit points
		if rateLimitRemaining < 2 {
			s.ContinueAt = res.RateLimit.ResetAt
			log.Printf("[i][%v] primary rate limit exceeded, continuing at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
			return false, nil
		}
		// Start by requesting the first page of search results staring from the cursor
		res, err = github.GetSshPublicKeys(ctx, client, compileQueryString(s.Cursor), "")
		if err != nil {
			if strings.Contains(err.Error(), "You have exceeded a secondary rate limit") {
				s.ContinueAt = time.Now().Add(1 * time.Minute)
				log.Printf("[i][%v] secondary rate limit exceeded, continuing at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
			} else {
				s.ContinueAt = time.Now().Add(1 * time.Hour)
			}
			return false, err
		}
		rateLimitRemaining = res.RateLimit.Remaining

		// Check if this iteration is the last one for now
		cursor, _ := time.Parse(time.RFC3339, s.Cursor)
		lastIteration := cursor.Add(timespan).After(time.Now()) && res.Search.UserCount < searchLimit

		wg.Add(1)
		go processResponse(ctx, s, *res, &wg)

		// Paginate through all search results until we reach the end (at most 1000 entries)
		for res.Search.PageInfo.HasNextPage {
			if rateLimitRemaining < 2 {
				updateCursor(ctx, s, res)
				s.ContinueAt = res.RateLimit.ResetAt
				log.Printf("[i][%v] primary rate limit exceeded, continuing at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
				return false, nil
			}
			res, err = github.GetSshPublicKeys(ctx, client, compileQueryString(s.Cursor), res.Search.PageInfo.EndCursor)
			if err != nil {
				if strings.Contains(err.Error(), "You have exceeded a secondary rate limit") {
					s.ContinueAt = time.Now().Add(1 * time.Minute)
					log.Printf("[i][%v] secondary rate limit exceeded, continuing at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
				} else {
					s.ContinueAt = time.Now().Add(1 * time.Hour)
				}
				return false, err
			}
			rateLimitRemaining = res.RateLimit.Remaining

			wg.Add(1)
			go processResponse(ctx, s, *res, &wg)
		}

		// Wait for processing to complete
		wg.Wait()
		// Update the cursor to the last user we have seen
		updateCursor(ctx, s, res)
		if lastIteration {
			return true, nil
		}
		// GitHub has secondary rate limits to prevent abuse
		// There is a non-documented limit on the number of search requests in short duration for the GraphQL API
		// We ensure that each iteration takes at least 20 seconds to try avoiding hitting this limit
		// This is not a perfect solution, but it should be good enough for now
		iterationEnd := time.Now()
		iterationDuration := iterationEnd.Sub(iterationStart)
		if iterationDuration < 20*time.Second {
			time.Sleep(20*time.Second - iterationDuration)
		}
	}
}
