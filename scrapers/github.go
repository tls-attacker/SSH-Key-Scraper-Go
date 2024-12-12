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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The timespan which we request in a single request from the search API (30 days)
const timespan = 30 * 24 * time.Hour

// The maximum number of users GitHub returns to a single search request (even with pagination)
const searchLimit = 1000

// The maximum number of requests we can make to the GitHub API per hour
const maxPrimaryRateLimit = 5000

type GitHubScraper struct {
	*Scraper
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

type githubTransport struct {
	token   string
	wrapped http.RoundTripper
}

func (t *githubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.token))
	return t.wrapped.RoundTrip(req)
}

func (s *GitHubScraper) newGraphQLClient() graphql.Client {
	httpClient := http.Client{
		Transport: &githubTransport{
			token:   s.getPlatformConfigString("token"),
			wrapped: http.DefaultTransport,
		},
	}
	return graphql.NewClient("https://api.github.com/graphql", &httpClient)
}

func (s *GitHubScraper) compileQueryString(cursor string) string {
	startDate, _ := time.Parse(time.RFC3339, cursor)
	endDate := startDate.Add(timespan)
	return fmt.Sprintf("created:%s..%s type:user sort:joined-asc",
		startDate.UTC().Format("2006-01-02T15:04:05Z"),
		endDate.UTC().Format("2006-01-02T15:04:05Z"))
}

func (s *GitHubScraper) updateCursor(ctx context.Context, res *github.GetSshPublicKeysResponse) {
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
		s.log("failed to save cursor: %v", true, err)
	}
	s.log("cursor updated, new cursor: %v", false, s.Cursor)
}

func (s *GitHubScraper) mapToUserEntry(user *github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser, existing *GitHubUserEntry) *GitHubUserEntry {
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

func (s *GitHubScraper) createUserIndex(ctx context.Context) error {
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
		return fmt.Errorf("failed to create user index: %w", err)
	}
	return nil
}

func (s *GitHubScraper) processResponse(ctx context.Context, res github.GetSshPublicKeysResponse, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, user := range res.Search.Nodes {
		if user, ok := user.(*github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser); ok {
			searchResult, err := s.Elasticsearch.Search().
				Index(s.UserIndex).
				Request(&search.Request{
					Query: &types.Query{
						Match: map[string]types.MatchQuery{
							"databaseId": {Query: strconv.Itoa(user.DatabaseId)},
						},
					},
				}).Do(ctx)
			if err != nil {
				// If anything goes wrong, we save the unprocessed user to a file
				err := s.saveUnprocessedUser(user.Login, user)
				if err != nil {
					panic(err)
				}
				continue
			}
			var entry *GitHubUserEntry
			if searchResult.Hits.Total.Value == 0 {
				entry = s.mapToUserEntry(user, nil)
				_, err = s.Elasticsearch.Index(s.UserIndex).
					Request(entry).
					Do(ctx)
				if err != nil {
					err := s.saveUnprocessedUser(user.Login, user)
					if err != nil {
						panic(err)
					}
					continue
				}
			} else {
				var existing *GitHubUserEntry
				if err = json.Unmarshal(searchResult.Hits.Hits[0].Source_, &existing); err != nil {
					err := s.saveUnprocessedUser(user.Login, user)
					if err != nil {
						panic(err)
					}
					continue
				}
				entry = s.mapToUserEntry(user, entry)
				_, err = s.Elasticsearch.Index(s.UserIndex).
					Id(*searchResult.Hits.Hits[0].Id_).
					Request(entry).
					Do(ctx)
				if err != nil {
					err := s.saveUnprocessedUser(user.Login, user)
					if err != nil {
						panic(err)
					}
					continue
				}
			}
		}
	}
}

func (s *GitHubScraper) handleApiError(err error) {
	if strings.Contains(err.Error(), "You have exceeded a secondary rate limit") {
		// If we hit the secondary rate limit, we wait for a minute before continuing
		s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("secondaryRateLimitCooldown"))
		s.log("secondary rate limit exceeded, continuing at %v", false, s.ContinueAt.Format(time.RFC3339))
	} else {
		// If we encounter any other error, we wait for an hour before continuing
		s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration("apiErrorCooldown"))
	}
}

func (s *GitHubScraper) Scrape(ctx context.Context) (bool, error) {
	if err := s.createUserIndex(ctx); err != nil {
		panic(err)
	}
	client := s.newGraphQLClient()
	if s.Cursor == "" {
		s.Cursor = s.getPlatformConfigString("initialCursor")
		if err := s.Save(ctx); err != nil {
			panic(err)
		}
	}

	s.log("starting scraping from %v", false, s.Cursor)

	wg := sync.WaitGroup{}
	var res *github.GetSshPublicKeysResponse
	var err error
	rateLimitRemaining := maxPrimaryRateLimit
	for {
		iterationStart := time.Now()
		// A single request usually costs between 1-2 rate limit points
		if rateLimitRemaining < 2 {
			s.ContinueAt = res.RateLimit.ResetAt
			s.log("primary rate limit exceeded, continuing at %v", false, s.ContinueAt.Format(time.RFC3339))
			return false, nil
		}
		// Start by requesting the first page of search results staring from the cursor
		res, err = github.GetSshPublicKeys(ctx, client, s.compileQueryString(s.Cursor), "")
		if err != nil {
			s.handleApiError(err)
			return false, err
		}
		rateLimitRemaining = res.RateLimit.Remaining

		// Check if this iteration is the last one for now
		cursor, _ := time.Parse(time.RFC3339, s.Cursor)
		lastIteration := cursor.Add(timespan).After(time.Now()) && res.Search.UserCount < searchLimit

		wg.Add(1)
		go s.processResponse(ctx, *res, &wg)

		// Paginate through all search results until we reach the end (at most 1000 entries)
		for res.Search.PageInfo.HasNextPage {
			if rateLimitRemaining < 2 {
				s.updateCursor(ctx, res)
				s.ContinueAt = res.RateLimit.ResetAt
				s.log("primary rate limit exceeded, continuing at %v", false, s.ContinueAt.Format(time.RFC3339))
				return false, nil
			}
			res, err = github.GetSshPublicKeys(ctx, client, s.compileQueryString(s.Cursor), res.Search.PageInfo.EndCursor)
			if err != nil {
				s.handleApiError(err)
				return false, err
			}
			rateLimitRemaining = res.RateLimit.Remaining

			wg.Add(1)
			go s.processResponse(ctx, *res, &wg)
		}

		// Wait for processing to complete
		wg.Wait()
		// Update the cursor to the last user we have seen
		s.updateCursor(ctx, res)
		if lastIteration {
			return true, nil
		}
		// GitHub has secondary rate limits to prevent abuse
		// There is a non-documented limit on the number of search requests in short duration for the GraphQL API
		// We ensure that each iteration takes at least 20 seconds to try avoiding hitting this limit
		// This is not a perfect solution, but it should be good enough for now
		iterationEnd := time.Now()
		iterationDuration := iterationEnd.Sub(iterationStart)
		if iterationDuration < s.MinimumIterationDuration {
			time.Sleep(s.MinimumIterationDuration - iterationDuration)
		}
	}
}
