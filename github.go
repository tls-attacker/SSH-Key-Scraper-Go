package main

import (
	"SSH-Key-Scraper/graphql/github"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Khan/genqlient/graphql"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"net/http"
	"strings"
	"sync"
	"time"
)

// requestedTimespanGithub specifies the timespan which we request in a single request from the search API (30 days)
const requestedTimespanGithub = 30 * 24 * time.Hour

// initialCursorGithub is the creation date of the first user account on GitHub (user id 1). It is used as the start point
// (or end point in case of reverse direction) for scraping runs
var initialCursorGithub = time.Date(2007, 10, 20, 05, 24, 19, 0, time.UTC)

// The maximum number of users GitHub returns to a single search request (even with pagination)
const searchLimit = 1000

// The maximum number of requests we can make to the GitHub API per hour
const maxPrimaryRateLimit = 5000

const (
	MetaGithubUserRemoteId         = "remoteId"
	MetaGithubUserCreatedAt        = "createdAt"
	MetaGithubUserUpdatedAt        = "updatedAt"
	MetaGithubPublicKeyFingerprint = "fingerprint"
)

type GithubScraper struct {
	*Scraper
}

type githubTransport struct {
	token   string
	wrapped http.RoundTripper
}

func (t *githubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.token))
	return t.wrapped.RoundTrip(req)
}

func (s *GithubScraper) getUserMetadataMapping() *types.ObjectProperty {
	return &types.ObjectProperty{
		Properties: map[string]types.Property{
			MetaGithubUserRemoteId:  types.NewKeywordProperty(),
			MetaGithubUserCreatedAt: types.NewDateProperty(),
			MetaGithubUserUpdatedAt: types.NewDateProperty(),
		},
	}
}

func (s *GithubScraper) getPublicKeyMetadataMapping() *types.ObjectProperty {
	return &types.ObjectProperty{
		Properties: map[string]types.Property{
			MetaGithubPublicKeyFingerprint: types.NewTextProperty(),
		},
	}
}

func (s *GithubScraper) newGraphQLClient() graphql.Client {
	httpClient := http.Client{
		Timeout: s.getPlatformConfigDuration(ConfigTimeout),
		Transport: &githubTransport{
			token:   s.getPlatformConfigString(ConfigToken),
			wrapped: http.DefaultTransport,
		},
	}
	return graphql.NewClient("https://api.github.com/graphql", &httpClient)
}

func (s *GithubScraper) compileQueryString(cursor string) string {
	if !s.getPlatformConfigBool(ConfigReverse) {
		startDate, _ := time.Parse(time.RFC3339, cursor)
		endDate := startDate.Add(requestedTimespanGithub)
		return fmt.Sprintf("created:%s..%s type:user sort:joined-asc",
			startDate.UTC().Format("2006-01-02T15:04:05Z"),
			endDate.UTC().Format("2006-01-02T15:04:05Z"))
	} else {
		endDate, _ := time.Parse(time.RFC3339, cursor)
		startDate := endDate.Add(-requestedTimespanGithub)
		return fmt.Sprintf("created:%s..%s type:user sort:joined-desc",
			startDate.UTC().Format("2006-01-02T15:04:05Z"),
			endDate.UTC().Format("2006-01-02T15:04:05Z"))
	}
}

func (s *GithubScraper) updateCursor(ctx context.Context, last *github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser) {
	s.Cursor = last.CreatedAt.Format(time.RFC3339)
	if err := s.Save(ctx); err != nil {
		s.log("failed to save cursor: %v", true, err)
	}
	s.log("cursor updated, new cursor: %v", false, s.Cursor)
}

func (s *GithubScraper) mapToUserEntry(user *github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser, existing *UserEntry) *UserEntry {
	now := time.Now()
	entry := &UserEntry{
		Username: user.Login,
		Metadata: map[string]any{
			MetaGithubUserRemoteId:  user.DatabaseId,
			MetaGithubUserCreatedAt: user.CreatedAt,
			MetaGithubUserUpdatedAt: user.UpdatedAt,
		},
		VisitedAt: now,
		Deleted:   false,
	}
	if existing != nil {
		entry.PublicKeys = existing.PublicKeys
	} else {
		entry.PublicKeys = []PublicKeyEntry{}
	}
	for _, key := range user.PublicKeys.Nodes {
		exists := false
		for _, existingKey := range entry.PublicKeys {
			if existingKey.Key == key.Key {
				exists = true
				// Update metadata
				existingKey.Metadata[MetaGithubPublicKeyFingerprint] = key.Fingerprint
				existingKey.Deleted = false
				existingKey.VisitedAt = now
			}
		}
		if !exists {
			entry.PublicKeys = append(entry.PublicKeys, PublicKeyEntry{
				Key: key.Key,
				Metadata: map[string]any{
					MetaGithubPublicKeyFingerprint: key.Fingerprint,
				},
				VisitedAt: time.Now(),
				Deleted:   false,
			})
		}
	}
	return entry
}

func (s *GithubScraper) processResponse(ctx context.Context, res github.GetSshPublicKeysResponse, wg *sync.WaitGroup) {
	// Allow wg to be nil in case processResponse is executed synchronously
	if wg != nil {
		defer wg.Done()
	}
	for _, user := range res.Search.Nodes {
		if user, ok := user.(*github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser); ok {
			searchResult, err := s.Elasticsearch.Search().
				Index(s.UserIndex).
				Request(&search.Request{
					Query: &types.Query{
						Match: map[string]types.MatchQuery{
							"username": {Query: user.Login},
						},
					},
				}).Preference("_local").Do(ctx)
			if err != nil {
				// If anything goes wrong, we save the unprocessed user to a file
				s.log("failed to search for user %v in elasticsearch: %v", true, user.Login, err)
				err := s.saveUnprocessedUser(user.Login, user)
				if err != nil {
					panic(err)
				}
				continue
			}
			var entry *UserEntry
			if searchResult.Hits.Total.Value == 0 {
				entry = s.mapToUserEntry(user, nil)
				_, err = s.Elasticsearch.Index(s.UserIndex).
					Request(entry).
					Do(ctx)
				if err != nil {
					s.log("failed to index user %v in elasticsearch: %v", true, user.Login, err)
					err := s.saveUnprocessedUser(user.Login, user)
					if err != nil {
						panic(err)
					}
					continue
				}
			} else {
				var existing UserEntry
				if err = json.Unmarshal(searchResult.Hits.Hits[0].Source_, &existing); err != nil {
					s.log("failed to unmarshal user %v from elasticsearch: %v", true, user.Login, err)
					err := s.saveUnprocessedUser(user.Login, user)
					if err != nil {
						panic(err)
					}
					continue
				}
				entry = s.mapToUserEntry(user, &existing)
				_, err = s.Elasticsearch.Index(s.UserIndex).
					Id(*searchResult.Hits.Hits[0].Id_).
					Request(entry).
					Do(ctx)
				if err != nil {
					s.log("failed to update user %v in elasticsearch: %v", true, user.Login, err)
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

func (s *GithubScraper) handleNotFoundError(ctx context.Context, queryString string, after string) {
	var data struct {
		Search struct {
			Nodes []any `json:"nodes"`
		} `json:"search"`
	}
	client := s.newGraphQLClient()
	req := &graphql.Request{
		OpName: "GetSshPublicKeys",
		Query:  github.GetSshPublicKeys_Operation,
		Variables: struct {
			Query string `json:"query"`
			After string `json:"after,omitempty"`
		}{
			Query: queryString,
			After: after,
		},
	}
	resp := &graphql.Response{Data: &data}
	// Make request but do not check the error as we can expect it to indicate the "Not Found" errors
	_ = client.MakeRequest(ctx, req, resp)
	for index, userNode := range data.Search.Nodes {
		// Check if userNode is nil (which is the case when the user is not found by GitHub for whatever reason)
		if userNode == nil {
			s.log("skipped nil user node at index %v (query: '%v' | after: '%v')", false, index, queryString, after)
			continue
		}
		var userObject map[string]any
		var ok bool
		if userObject, ok = userNode.(map[string]any); !ok {
			s.log("failed to parse user object from defect response while trying to handle not found error, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		// userNode should be okay to process, try to recover data from the node
		var publicKeyObject map[string]any
		if publicKeyObject, ok = userObject["publicKeys"].(map[string]any); !ok {
			s.log("failed to parse public key response object from defect response while trying to handle not found error, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		var publicKeyTotalCount float64
		if publicKeyTotalCount, ok = publicKeyObject["totalCount"].(float64); !ok {
			s.log("failed to parse total public key count from defect response while trying to handle not found error, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		publicKeyNodes := make([]map[string]any, 0, int(publicKeyTotalCount))
		var publicKeyNodeArray []any
		if publicKeyNodeArray, ok = publicKeyObject["nodes"].([]any); !ok {
			s.log("failed to parse public key node array from defect response while trying to handle not found error, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		for nodeIndex, node := range publicKeyNodeArray {
			var mappedNode map[string]any
			if mappedNode, ok = node.(map[string]any); !ok {
				s.log("failed to parse public key node from defect response while trying to handle not found error, skipping public key %v for user at index %v (query: '%v' | after: '%v')", true, nodeIndex, index, queryString, after)
				continue
			}
			publicKeyNodes = append(publicKeyNodes, mappedNode)
		}
		publicKeys := github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUserPublicKeysPublicKeyConnection{
			TotalCount: int(publicKeyTotalCount),
		}
		for pkIndex, publicKeyNode := range publicKeyNodes {
			var pkKey string
			if pkKey, ok = publicKeyNode["key"].(string); !ok {
				s.log("failed to parse public key from defect response while trying to handle not found error, skipping public key at index %v from user node at index %v (query: '%v' | after: '%v')", true, pkIndex, index, queryString, after)
				continue
			}
			var pkFingerprint string
			if pkFingerprint, ok = publicKeyNode["fingerprint"].(string); !ok {
				s.log("failed to parse public key fingerprint from defect response while trying to handle not found error, skipping public key at index %v from user node at index %v (query: '%v' | after: '%v')", true, pkIndex, index, queryString, after)
				continue
			}
			publicKey := github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUserPublicKeysPublicKeyConnectionNodesPublicKey{
				Key:         pkKey,
				Fingerprint: pkFingerprint,
			}
			publicKeys.Nodes = append(publicKeys.Nodes, publicKey)
		}
		var databaseId float64
		if databaseId, ok = userObject["databaseId"].(float64); !ok {
			s.log("failed to parse database id from defect response while trying to handle not found errors, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		var login string
		if login, ok = userObject["login"].(string); !ok {
			s.log("failed to parse login from defect response while trying to handle not found errors, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		var createdAtString string
		if createdAtString, ok = userObject["createdAt"].(string); !ok {
			s.log("failed to parse createdAt from defect response while trying to handle not found errors, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtString)
		if err != nil {
			s.log("failed to parse createdAt time from defect response while trying to handle not found errors, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		var updatedAtString string
		if updatedAtString, ok = userObject["updatedAt"].(string); !ok {
			s.log("failed to parse updatedAt from defect response while trying to handle not found errors, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		updatedAt, err := time.Parse(time.RFC3339, updatedAtString)
		if err != nil {
			s.log("failed to parse updatedAt time from defect response while trying to handle not found errors, skipping user node at index %v (query: '%v' | after: '%v')", true, index, queryString, after)
			continue
		}
		user := github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser{
			DatabaseId: int(databaseId),
			Login:      login,
			CreatedAt:  createdAt,
			UpdatedAt:  updatedAt,
			PublicKeys: publicKeys,
		}
		// Store recovered users one by one in order to be able to update the cursor after each insertion
		recoveredResponse := github.GetSshPublicKeysResponse{}
		recoveredResponse.Search.Nodes = append(recoveredResponse.Search.Nodes, &user)
		s.processResponse(ctx, recoveredResponse, nil)
		s.Cursor = user.CreatedAt.Format(time.RFC3339)
		if err = s.Save(ctx); err != nil {
			s.log("failed to save cursor: %v", true, err)
		}
		s.log("recovered single user at index %v from defect API response, new cursor: %v", false, index, s.Cursor)
	}
}

func (s *GithubScraper) handleApiError(ctx context.Context, err error, queryString string, after string) {
	var gqlerr gqlerror.List
	if strings.Contains(err.Error(), "You have exceeded a secondary rate limit") {
		// If we hit the secondary rate limit, we wait for a minute before continuing
		s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigSecondaryRateLimitCooldown))
		s.log("secondary rate limit exceeded, continuing at %v", false, s.ContinueAt.Format(time.RFC3339))
	} else if errors.As(err, &gqlerr) {
		// Check if the error has been caused by GitHub being unable to find (?!) some users
		// This seems to be a bug or database inconsistency on GitHub's end causing the API to return null,
		// thus returning errors instead. We work around this to avoid getting stuck while scraping.
		isOnlyNotFound := true
		for _, e := range gqlerr {
			isOnlyNotFound = isOnlyNotFound && e.Message == "Not Found"
		}
		if !isOnlyNotFound {
			// Others errors are present as well, fallback to generic error routine (sleeping the configured delay and retry)
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			return
		}
		// We now know that all errors are "Not Found" errors and that these errors occur when querying queryString
		// with the after cursor. Parse request manually to extract non-faulty data that is present in the request.
		s.log("encountered \"Not Found\" errors (query: '%v' | after: '%v'), trying to recover as many user nodes as possible from current page (best effort)", false, queryString, after)
		s.handleNotFoundError(ctx, queryString, after)
		// Continue immediately
		s.ContinueAt = time.Now()
	} else {
		// If we encounter any other error, we wait for an hour before continuing
		s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
	}
}

func (s *GithubScraper) Scrape(ctx context.Context) (bool, error) {
	client := s.newGraphQLClient()
	if s.Cursor == "" {
		s.Cursor = initialCursorGithub.Format(time.RFC3339)
		if err := s.Save(ctx); err != nil {
			panic(err)
		}
	}

	s.log("starting scraping from %v", false, s.Cursor)

	wg := sync.WaitGroup{}
	var res *github.GetSshPublicKeysResponse
	var err error
	rateLimitRemaining := maxPrimaryRateLimit
	minimumIterationDuration := s.getPlatformConfigDuration(ConfigMinimumIterationDuration)
	for {
		iterationStart := time.Now()
		// A single request usually costs between 1-2 rate limit points
		if rateLimitRemaining < 2 {
			s.ContinueAt = res.RateLimit.ResetAt.Add(1 * time.Minute)
			s.log("primary rate limit exceeded, continuing at %v", false, s.ContinueAt.Format(time.RFC3339))
			return false, nil
		}
		// Start by requesting the first page of search results staring from the cursor
		queryString := s.compileQueryString(s.Cursor)
		res, err = github.GetSshPublicKeys(ctx, client, queryString, "")
		if err != nil {
			s.handleApiError(ctx, err, queryString, "")
			return false, err
		}
		rateLimitRemaining = res.RateLimit.Remaining

		// Check if this iteration is the last one for now
		cursor, _ := time.Parse(time.RFC3339, s.Cursor)
		var lastIteration bool
		if !s.getPlatformConfigBool(ConfigReverse) {
			lastIteration = cursor.Add(requestedTimespanGithub).After(time.Now()) && res.Search.UserCount < searchLimit
		} else {
			lastIteration = cursor.Add(-requestedTimespanGithub).Before(initialCursorGithub) && res.Search.UserCount < searchLimit
		}

		wg.Add(1)
		go s.processResponse(ctx, *res, &wg)

		// Paginate through all search results until we reach the end (at most 1000 entries)
		for res.Search.PageInfo.HasNextPage {
			if rateLimitRemaining < 2 {
				if len(res.Search.Nodes) > 0 {
					s.updateCursor(ctx, res.Search.Nodes[len(res.Search.Nodes)-1].(*github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser))
				} else {
					s.log("unable to update cursor, search result does not contain any nodes", true)
				}
				s.ContinueAt = res.RateLimit.ResetAt.Add(1 * time.Minute)
				s.log("primary rate limit exceeded, continuing at %v", false, s.ContinueAt.Format(time.RFC3339))
				return false, nil
			}
			queryString = s.compileQueryString(s.Cursor)
			after := res.Search.PageInfo.EndCursor
			res, err = github.GetSshPublicKeys(ctx, client, queryString, after)
			if err != nil {
				s.handleApiError(ctx, err, queryString, after)
				return false, err
			}
			rateLimitRemaining = res.RateLimit.Remaining

			wg.Add(1)
			go s.processResponse(ctx, *res, &wg)
		}

		// Wait for processing to complete
		wg.Wait()
		// Update the cursor to the last user we have seen
		if len(res.Search.Nodes) > 0 {
			s.updateCursor(ctx, res.Search.Nodes[len(res.Search.Nodes)-1].(*github.GetSshPublicKeysSearchSearchResultItemConnectionNodesUser))
		} else {
			// This can only happen if there is an API error or the initial cursor is not chosen appropriately
			// Skipping an entire timeframe here may yield unexpected results or incomplete results, therefore we return an error and try again later
			s.log("unable to update cursor, search result does not contain any nodes", true)
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			return false, fmt.Errorf("unable to update cursor, search result does not contain any nodes")
		}
		if lastIteration {
			return true, nil
		}
		// GitHub has secondary rate limits to prevent abuse
		// There is a non-documented limit on the number of search requests in short duration for the GraphQL API
		// We ensure that each iteration takes at least 20 seconds to try avoiding hitting this limit
		// This is not a perfect solution, but it should be good enough for now
		iterationEnd := time.Now()
		iterationDuration := iterationEnd.Sub(iterationStart)
		if iterationDuration < minimumIterationDuration {
			time.Sleep(minimumIterationDuration - iterationDuration)
		}
	}
}
