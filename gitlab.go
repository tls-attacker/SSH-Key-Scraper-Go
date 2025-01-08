package main

import (
	"SSH-Key-Scraper/graphql/gitlab"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Khan/genqlient/graphql"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MetaGitlabUserRemoteId       = "remoteId"
	MetaGitlabUserState          = "state"
	MetaGitlabUserCreatedAt      = "createdAt"
	MetaGitlabUserLastActivityOn = "lastActivityOn"
	MetaGitlabPublicKeyRemoteId  = "remoteId"
	MetaGitlabPublicKeyTitle     = "title"
	MetaGitlabPublicKeyUsageType = "usageType"
	MetaGitlabPublicKeyCreatedAt = "createdAt"
	MetaGitlabPublicKeyExpiresAt = "expiresAt"
)

type GitlabSshKeysApiEntry struct {
	Id        int64     `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt *string   `json:"expires_at"`
	Key       string    `json:"key"`
	UsageType string    `json:"usage_type"`
}

type GitlabScraper struct {
	*Scraper
}

type gitlabTransport struct {
	token   string
	graphql bool
	wrapped http.RoundTripper
}

func (s *GitlabScraper) getUserMetadataMapping() *types.ObjectProperty {
	return &types.ObjectProperty{
		Properties: map[string]types.Property{
			MetaGitlabUserRemoteId:       types.NewKeywordProperty(),
			MetaGitlabUserState:          types.NewKeywordProperty(),
			MetaGitlabUserCreatedAt:      types.NewDateProperty(),
			MetaGitlabUserLastActivityOn: types.NewDateProperty(),
		},
	}
}

func (s *GitlabScraper) getPublicKeyMetadataMapping() *types.ObjectProperty {
	format := "yyyyyy-MM-dd'T'HH:mm:ss[.SSS]XXX||yyyyy-MM-dd'T'HH:mm:ss[.SSS]XXX||strict_date_optional_time||epoch_millis"
	expiresAtType := types.NewDateProperty()
	expiresAtType.Format = &format
	return &types.ObjectProperty{
		Properties: map[string]types.Property{
			MetaGitlabPublicKeyRemoteId:  types.NewLongNumberProperty(),
			MetaGitlabPublicKeyTitle:     types.NewTextProperty(),
			MetaGitlabPublicKeyUsageType: types.NewKeywordProperty(),
			MetaGitlabPublicKeyCreatedAt: types.NewDateProperty(),
			MetaGitlabPublicKeyExpiresAt: expiresAtType,
		},
	}
}

func (t *gitlabTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.graphql {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.token))
	} else {
		req.Header.Set("PRIVATE-TOKEN", t.token)
	}
	return t.wrapped.RoundTrip(req)
}

func (s *GitlabScraper) newGraphQLClient() graphql.Client {
	httpClient := http.Client{
		Timeout: s.getPlatformConfigDuration(ConfigTimeout),
		Transport: &gitlabTransport{
			token:   s.getPlatformConfigString(ConfigToken),
			graphql: true,
			wrapped: http.DefaultTransport,
		},
	}
	return graphql.NewClient("https://gitlab.com/api/graphql", &httpClient)
}

func (s *GitlabScraper) newRestClient() *http.Client {
	return &http.Client{
		Timeout: s.getPlatformConfigDuration(ConfigTimeout),
		Transport: &gitlabTransport{
			token:   s.getPlatformConfigString(ConfigToken),
			graphql: false,
			wrapped: http.DefaultTransport,
		},
	}
}

func (s *GitlabScraper) gidToUserId(gid string) (int, error) {
	parts := strings.Split(gid, "/")
	return strconv.Atoi(parts[len(parts)-1])
}

func (s *GitlabScraper) updateCursor(ctx context.Context, last *gitlab.GetUsersUsersUserCoreConnectionNodesUserCore) {
	if last == nil {
		return
	}
	type cursor struct {
		CreatedAt string `json:"created_at"`
		Id        string `json:"id"`
	}
	userId, err := s.gidToUserId(last.Id)
	if err != nil {
		s.log("failed to parse user id from gid: %v", true, err)
		return
	}
	cursorData := cursor{
		CreatedAt: last.CreatedAt.Format("2006-01-02T15:04:05Z"),
		Id:        strconv.Itoa(userId),
	}
	cursorJson, _ := json.Marshal(cursorData)
	s.Cursor = base64.StdEncoding.EncodeToString(cursorJson)
	if err := s.Save(ctx); err != nil {
		s.log("failed to save cursor: %v", true, err)
	}
	s.log("cursor updated, new cursor: %v", false, s.Cursor)
}

func (s *GitlabScraper) mapToUserEntry(user *gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, publicKeys *[]GitlabSshKeysApiEntry, existing *UserEntry) *UserEntry {
	now := time.Now()
	entry := &UserEntry{
		Username: user.Username,
		Metadata: map[string]any{
			MetaGitlabUserRemoteId:       user.Id,
			MetaGitlabUserState:          string(user.State),
			MetaGitlabUserCreatedAt:      user.CreatedAt,
			MetaGitlabUserLastActivityOn: user.LastActivityOn,
		},
		VisitedAt: now,
		Deleted:   false,
	}
	if existing != nil {
		entry.PublicKeys = existing.PublicKeys
	} else {
		entry.PublicKeys = []PublicKeyEntry{}
	}
	for _, key := range *publicKeys {
		exists := false
		for _, existingKey := range entry.PublicKeys {
			if existingKey.Key == key.Key {
				exists = true
				// Update metadata
				existingKey.Metadata[MetaGitlabPublicKeyRemoteId] = key.Id
				existingKey.Metadata[MetaGitlabPublicKeyTitle] = key.Title
				existingKey.Metadata[MetaGitlabPublicKeyUsageType] = key.UsageType
				existingKey.Metadata[MetaGitlabPublicKeyCreatedAt] = key.CreatedAt
				existingKey.Metadata[MetaGitlabPublicKeyExpiresAt] = key.ExpiresAt
				existingKey.Deleted = false
				existingKey.VisitedAt = now
			}
		}
		if !exists {
			entry.PublicKeys = append(entry.PublicKeys, PublicKeyEntry{
				Key: key.Key,
				Metadata: map[string]any{
					MetaGitlabPublicKeyRemoteId:  key.Id,
					MetaGitlabPublicKeyTitle:     key.Title,
					MetaGitlabPublicKeyUsageType: key.UsageType,
					MetaGitlabPublicKeyCreatedAt: key.CreatedAt,
					MetaGitlabPublicKeyExpiresAt: key.ExpiresAt,
				},
				VisitedAt: now,
				Deleted:   false,
			})
		}
	}
	return entry
}

func (s *GitlabScraper) processResponse(ctx context.Context, user *gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, publicKeys *[]GitlabSshKeysApiEntry) {
	searchResult, err := s.Elasticsearch.Search().
		Index(s.UserIndex).
		Request(&search.Request{
			Query: &types.Query{
				Match: map[string]types.MatchQuery{
					"username": {Query: user.Username},
				},
			},
		}).Preference("_local").Do(ctx)
	if err != nil {
		// If anything goes wrong, we save the unprocessed user to a file
		s.log("failed to search for user %v in elasticsearch: %v", true, user.Username, err)
		err := s.saveUnprocessedUser(user.Username, user)
		if err != nil {
			panic(err)
		}
		return
	}
	var entry *UserEntry
	if searchResult.Hits.Total.Value == 0 {
		entry = s.mapToUserEntry(user, publicKeys, nil)
		_, err = s.Elasticsearch.Index(s.UserIndex).
			Request(entry).
			Do(ctx)
		if err != nil {
			s.log("failed to index user %v in elasticsearch: %v", true, user.Username, err)
			err := s.saveUnprocessedUser(user.Username, user)
			if err != nil {
				panic(err)
			}
			return
		}
	} else {
		var existing UserEntry
		if err = json.Unmarshal(searchResult.Hits.Hits[0].Source_, &existing); err != nil {
			s.log("failed to unmarshal user %v from elasticsearch: %v", true, user.Username, err)
			err := s.saveUnprocessedUser(user.Username, user)
			if err != nil {
				panic(err)
			}
			return
		}
		entry = s.mapToUserEntry(user, publicKeys, &existing)
		_, err = s.Elasticsearch.Index(s.UserIndex).
			Id(*searchResult.Hits.Hits[0].Id_).
			Request(entry).
			Do(ctx)
		if err != nil {
			s.log("failed to update user %v in elasticsearch: %v", true, user.Username, err)
			err := s.saveUnprocessedUser(user.Username, user)
			if err != nil {
				panic(err)
			}
			return
		}
	}
}

func (s *GitlabScraper) publicKeyWorker(ctx context.Context, users <-chan gitlab.GetUsersUsersUserCoreConnectionNodesUserCore, failures chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	restClient := s.newRestClient()
	maxRetries := s.getPlatformConfigInt(ConfigMaxRetries)
	for user := range users {
		userId, err := s.gidToUserId(user.Id)
		if err != nil {
			s.log("failed to parse user id from gid: %v", true, err)
			if err := s.saveUnprocessedUser(user.Username, user); err != nil {
				panic(err)
			}
			continue
		}
		retry := 0
		var res *http.Response
		for res == nil || err != nil {
			res, err = restClient.Get(fmt.Sprintf("https://gitlab.com/api/v4/users/%v/keys", strconv.Itoa(userId)))
			if err != nil {
				retry++
				if retry <= maxRetries {
					s.log("failed to retrieve user's public keys from gitlab, retrying %v/%v: %v", true, retry, maxRetries, err)
					continue
				}
				// If we encounter any error after exceeding maxRetries, we wait for the configured duration before continuing
				s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
				failures <- err
				return
			}
		}
		if res.StatusCode != 200 {
			if res.StatusCode == 429 {
				// If we hit the rate limit, we wait for the rate limit reset time before continuing
				if resetTime, err := time.Parse(time.RFC1123, res.Header.Get("RateLimit-ResetTime")); err == nil {
					s.ContinueAt = resetTime.Add(1 * time.Minute)
					s.log("hit rate limit, continuing at %v", false, s.ContinueAt)
				} else {
					s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
					s.log("hit rate limit but failed to parse rate limit reset time, continuing at %v", true, s.ContinueAt)
				}
				failures <- fmt.Errorf("rate limit hit")
				return
			} else if res.StatusCode == 404 {
				// If the API cannot find a user by the given username, we skip the user
				s.log("user %v not found in REST API, continuing", true, user.Username)
				if err := s.saveUnprocessedUser(user.Username, user); err != nil {
					panic(err)
				}
				continue
			} else if res.StatusCode == 500 {
				// Rarely retrieving SSH public keys for a user fails repeatedly with status code 500 for unknown reasons
				// In this case, we store the user as unprocessed for further analysis and proceed scraping the next user
				// Note that this behavior is distinct from 503 status codes
				s.log("encountered status code 500 while retrieving public keys for user %v, something might be wrong on the other end", true, user.Username)
				if err := s.saveUnprocessedUser(user.Username, user); err != nil {
					panic(err)
				}
				continue
			}
			// If we encounter any unknown error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			failures <- fmt.Errorf("unexpected status code while retrieving public keys for user %v: %v", user.Username, res.StatusCode)
			return
		}
		if res.Header.Get("Content-Type") != "application/json" {
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			failures <- fmt.Errorf("unexpected content type: %v", res.Header.Get("Content-Type"))
			return
		}
		var publicKeys []GitlabSshKeysApiEntry
		if err := json.NewDecoder(res.Body).Decode(&publicKeys); err != nil {
			// When we fail to decode the public keys due to some weird behaviour of the API, we continue with the next user
			s.log("failed to decode public keys from json for user %v: %v", true, user.Username, err)
			if err := s.saveUnprocessedUser(user.Username, user); err != nil {
				panic(err)
			}
			continue
		}
		s.processResponse(ctx, &user, &publicKeys)
	}
}

func (s *GitlabScraper) Scrape(ctx context.Context) (bool, error) {
	gqlClient := s.newGraphQLClient()

	s.log("starting scraping from %v", false, s.Cursor)

	wg := sync.WaitGroup{}
	concurrentRequests := s.getPlatformConfigInt(ConfigConcurrentRequests)
	retry := 0
	maxRetries := s.getPlatformConfigInt(ConfigMaxRetries)
	minimumIterationDuration := s.getPlatformConfigDuration(ConfigMinimumIterationDuration)
	var res *gitlab.GetUsersResponse
	var err error
	for {
		iterationStart := time.Now()
		// Start by fetching the next page of users
		if !s.getPlatformConfigBool(ConfigReverse) {
			res, err = gitlab.GetUsers(ctx, gqlClient, s.Cursor, gitlab.Sort_CREATED_ASC)
		} else {
			res, err = gitlab.GetUsers(ctx, gqlClient, s.Cursor, gitlab.Sort_CREATED_DESC)
		}
		if err != nil {
			retry++
			if retry <= maxRetries {
				s.log("failed to retrieve next batch of users from gitlab, retrying %v/%v: %v", true, retry, maxRetries, err)
				continue
			}
			// If we encounter any error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			return false, err
		}
		retry = 0

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
		if iterationDuration < minimumIterationDuration {
			time.Sleep(minimumIterationDuration - iterationDuration)
		}
	}
}
