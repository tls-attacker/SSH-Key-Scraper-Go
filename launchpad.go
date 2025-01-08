package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// requestedTimespanLaunchpad specifies the timespan which we request in a single request from the search API (72 hours)
const requestedTimespanLaunchpad = 24 * time.Hour

// initialCursorLaunchpad is the creation date of the first user account on Launchpad. It is used as the start point
// (or end point in case of reverse direction) for scraping runs
var initialCursorLaunchpad = time.Date(2005, 6, 15, 02, 17, 43, 0, time.UTC)

const (
	MetaLaunchpadUserIsValid       = "isValid"
	MetaLaunchpadUserIsTeam        = "isTeam"
	MetaLaunchpadUserStatus        = "status"
	MetaLaunchpadUserVisibility    = "visibility"
	MetaLaunchpadUserCreatedAt     = "createdAt"
	MetaLaunchpadPublicKeyRemoteId = "remoteId"
	MetaLaunchpadPublicKeyKeyType  = "keyType"
	MetaLaunchpadPublicKeyComment  = "comment"
)

type LaunchpadPeopleApiResponse struct {
	Start              int                       `json:"start"`
	TotalSizeLink      *string                   `json:"total_size_link"`
	TotalSize          *int                      `json:"total_size"`
	NextCollectionLink *string                   `json:"next_collection_link"`
	PrevCollectionLink *string                   `json:"prev_collection_link"`
	Entries            []LaunchpadPeopleApiEntry `json:"entries"`
}

type LaunchpadPeopleApiEntry struct {
	// The API returns more fields, but we only need to parse these
	SelfLink              string    `json:"self_link"`
	Name                  string    `json:"name"`
	IsValid               bool      `json:"is_valid"`
	IsTeam                bool      `json:"is_team"`
	AccountStatus         string    `json:"account_status"`
	Visibility            string    `json:"visibility"`
	DateCreated           time.Time `json:"date_created"`
	SshKeysCollectionLink string    `json:"sshkeys_collection_link"`
}

type LaunchpadSshKeysApiResponse struct {
	Start              int                        `json:"start"`
	TotalSizeLink      *string                    `json:"total_size_link"`
	TotalSize          *int                       `json:"total_size"`
	NextCollectionLink *string                    `json:"next_collection_link"`
	PrevCollectionLink *string                    `json:"prev_collection_link"`
	Entries            []LaunchpadSshKeysApiEntry `json:"entries"`
}

type LaunchpadSshKeysApiEntry struct {
	// The API returns more fields, but we only need to parse these
	SelfLink string `json:"self_link"`
	KeyType  string `json:"keytype"`
	KeyText  string `json:"keytext"`
	Comment  string `json:"comment"`
}

type LaunchpadScraper struct {
	*Scraper
}

func (s *LaunchpadScraper) getUserMetadataMapping() *types.ObjectProperty {
	return &types.ObjectProperty{
		Properties: map[string]types.Property{
			MetaLaunchpadUserIsValid:    types.NewBooleanProperty(),
			MetaLaunchpadUserIsTeam:     types.NewBooleanProperty(),
			MetaLaunchpadUserStatus:     types.NewKeywordProperty(),
			MetaLaunchpadUserVisibility: types.NewKeywordProperty(),
			MetaLaunchpadUserCreatedAt:  types.NewDateProperty(),
		},
	}
}

func (s *LaunchpadScraper) getPublicKeyMetadataMapping() *types.ObjectProperty {
	return &types.ObjectProperty{
		Properties: map[string]types.Property{
			MetaLaunchpadPublicKeyRemoteId: types.NewIntegerNumberProperty(),
			MetaLaunchpadPublicKeyKeyType:  types.NewKeywordProperty(),
			MetaLaunchpadPublicKeyComment:  types.NewTextProperty(),
		},
	}
}

func (s *LaunchpadScraper) newHttpClient() *http.Client {
	return &http.Client{
		Timeout: s.getPlatformConfigDuration(ConfigTimeout),
	}
}

func (s *LaunchpadScraper) selfLinkToApiId(selfLink string) (int, error) {
	// The self link is in the format "https://api.launchpad.net/devel/~USERNAME/+ssh-keys/API_ID"
	// We extract the API ID and return it
	parts := strings.Split(selfLink, "/")
	return strconv.Atoi(parts[len(parts)-1])
}

func (s *LaunchpadScraper) updateCursor(ctx context.Context) {
	// We only call this function after a successful scrape, so we can assume that we processed all data in the current
	// timespan. We can therefore safely move the cursor to the next timespan.
	current, _ := time.Parse(time.RFC3339, s.Cursor)
	if !s.getPlatformConfigBool(ConfigReverse) {
		current = current.Add(requestedTimespanLaunchpad)
	} else {
		current = current.Add(-requestedTimespanLaunchpad)
	}
	// If the new cursor is in the future, we set the cursor to the current time
	now := time.Now()
	if current.After(now) {
		current = now
	}
	s.Cursor = current.Format(time.RFC3339)
	if err := s.Save(ctx); err != nil {
		s.log("failed to save cursor: %v", true, err)
	}
	s.log("cursor updated, new cursor: %v", false, s.Cursor)
}

func (s *LaunchpadScraper) mapToUserEntry(apiUser *LaunchpadPeopleApiEntry, apiSshKeys *[]LaunchpadSshKeysApiEntry, existing *UserEntry) *UserEntry {
	now := time.Now()
	entry := &UserEntry{
		Username: apiUser.Name,
		Metadata: map[string]any{
			MetaLaunchpadUserIsValid:    apiUser.IsValid,
			MetaLaunchpadUserIsTeam:     apiUser.IsTeam,
			MetaLaunchpadUserStatus:     apiUser.AccountStatus,
			MetaLaunchpadUserVisibility: apiUser.Visibility,
			MetaLaunchpadUserCreatedAt:  apiUser.DateCreated,
		},
		VisitedAt: now,
		Deleted:   false,
	}
	if existing != nil {
		entry.PublicKeys = existing.PublicKeys
	} else {
		entry.PublicKeys = []PublicKeyEntry{}
	}
	for _, key := range *apiSshKeys {
		keyText := strings.TrimSpace(key.KeyText)
		keyText = strings.ReplaceAll(keyText, "\r", "")
		keyText = strings.ReplaceAll(keyText, "\n", "")
		apiId, err := s.selfLinkToApiId(key.SelfLink)
		if err != nil {
			apiId = 0
		}
		exists := false
		for _, existingKey := range entry.PublicKeys {
			// We do not use the API ID for comparison here, as it is not documented whether it is unique
			if existingKey.Key == keyText {
				exists = true
				existingKey.Metadata[MetaLaunchpadPublicKeyRemoteId] = apiId
				existingKey.Metadata[MetaLaunchpadPublicKeyKeyType] = key.KeyType
				existingKey.Metadata[MetaLaunchpadPublicKeyComment] = key.Comment
				existingKey.Deleted = false
				existingKey.VisitedAt = now
			}
		}
		if !exists {
			entry.PublicKeys = append(entry.PublicKeys, PublicKeyEntry{
				Key: keyText,
				Metadata: map[string]any{
					MetaLaunchpadPublicKeyRemoteId: apiId,
					MetaLaunchpadPublicKeyKeyType:  key.KeyType,
					MetaLaunchpadPublicKeyComment:  key.Comment,
				},
				VisitedAt: now,
				Deleted:   false,
			})
		}
	}
	return entry
}

func (s *LaunchpadScraper) processResponse(ctx context.Context, user *LaunchpadPeopleApiEntry, publicKeys *[]LaunchpadSshKeysApiEntry) {
	searchResult, err := s.Elasticsearch.Search().
		Index(s.UserIndex).
		Request(&search.Request{
			Query: &types.Query{
				Match: map[string]types.MatchQuery{
					"username": {Query: user.Name},
				},
			},
		}).Preference("_local").Do(ctx)
	if err != nil {
		// If anything goes wrong, we save the unprocessed user to a file
		s.log("failed to search for user %v in elasticsearch: %v", true, user.Name, err)
		err := s.saveUnprocessedUser(user.Name, user)
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
			s.log("failed to index user %v in elasticsearch: %v", true, user.Name, err)
			err := s.saveUnprocessedUser(user.Name, user)
			if err != nil {
				panic(err)
			}
			return
		}
	} else {
		var existing UserEntry
		if err = json.Unmarshal(searchResult.Hits.Hits[0].Source_, &existing); err != nil {
			s.log("failed to unmarshal user %v from elasticsearch: %v", true, user.Name, err)
			err := s.saveUnprocessedUser(user.Name, user)
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
			s.log("failed to update user %v in elasticsearch: %v", true, user.Name, err)
			err := s.saveUnprocessedUser(user.Name, user)
			if err != nil {
				panic(err)
			}
			return
		}
	}
}

func (s *LaunchpadScraper) publicKeyWorker(ctx context.Context, users <-chan LaunchpadPeopleApiEntry, failures chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	httpClient := s.newHttpClient()
	maxRetries := s.getPlatformConfigInt(ConfigMaxRetries)
	for user := range users {
		retry := 0
		var res *http.Response
		var err error
		for res == nil || err != nil {
			res, err = httpClient.Get(user.SshKeysCollectionLink)
			if err != nil || res.Header.Get("Content-Type") != "application/json" {
				// Any non-JSON response is most likely caused by timeouts on Launchpad's side
				// We therefore retry the request a few times before giving up
				retry++
				if retry <= maxRetries {
					s.log("failed to retrieve public keys for user %v from launchpad, retrying %v/%v", true, user.Name, retry, maxRetries)
					continue
				}
				// If we encounter any error after exceeding maxRetries, we wait for the configured duration before continuing
				s.log("exceeded max retries while retrieving public keys for user %v, giving up for now", true, s.Cursor)
				s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
				failures <- err
				return
			}
		}
		if res.StatusCode != 200 {
			if res.StatusCode == 429 {
				// Launchpad does not have a publicly documented rate limit, but we pay respect to the status code anyway
				// If we hit the rate limit, we wait for the rate limit reset time before continuing
				s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
				s.log("hit rate limit, continuing at %v", true, s.ContinueAt)
				failures <- fmt.Errorf("rate limit hit")
				return
			} else if res.StatusCode == 404 {
				// If the API cannot find a user by the given username, we skip the user
				s.log("public keys for user %v not found in api, continuing", true, user.Name)
				if err := s.saveUnprocessedUser(user.Name, user); err != nil {
					panic(err)
				}
				continue
			}
			// If we encounter any unknown error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			failures <- fmt.Errorf("unexpected status code while retrieving public keys for user %v: %v", user.Name, res.StatusCode)
			return
		}
		var publicKeys LaunchpadSshKeysApiResponse
		if err := json.NewDecoder(res.Body).Decode(&publicKeys); err != nil {
			// When we fail to decode the public keys due to some weird behaviour of the API, we continue with the next user
			s.log("failed to decode sshkeys api response from json for user %v: %v", true, user.Name, err)
			if err := s.saveUnprocessedUser(user.Name, user); err != nil {
				panic(err)
			}
			continue
		}
		s.processResponse(ctx, &user, &publicKeys.Entries)
	}
}

func (s *LaunchpadScraper) Scrape(ctx context.Context) (bool, error) {
	httpClient := s.newHttpClient()
	if s.Cursor == "" {
		s.Cursor = initialCursorLaunchpad.Format(time.RFC3339)
		if err := s.Save(ctx); err != nil {
			panic(err)
		}
	}

	s.log("starting scraping from %v", false, s.Cursor)

	wg := sync.WaitGroup{}
	concurrentRequests := s.getPlatformConfigInt(ConfigConcurrentRequests)
	retry := 0
	maxRetries := s.getPlatformConfigInt(ConfigMaxRetries)
	minimumIterationDuration := s.getPlatformConfigDuration(ConfigMinimumIterationDuration)
	var requestUrl string
	page := 0
	lastTimespan := false
	for {
		iterationStart := time.Now()

		// If we are on the first page in the current timespan, we compose the request URL from start date to end date
		if page == 0 {
			if !s.getPlatformConfigBool(ConfigReverse) {
				startDate, _ := time.Parse(time.RFC3339, s.Cursor)
				endDate := startDate.Add(requestedTimespanLaunchpad)
				lastTimespan = endDate.After(time.Now())
				requestUrl = fmt.Sprintf("https://api.launchpad.net/devel/people?created_after=\"%s\"&created_before=\"%s\"&text=\"\"&ws.op=findPerson&ws.size=100",
					url.QueryEscape(startDate.UTC().Format("2006-01-02T15:04:05Z")),
					url.QueryEscape(endDate.UTC().Format("2006-01-02T15:04:05Z")))
			} else {
				endDate, _ := time.Parse(time.RFC3339, s.Cursor)
				startDate := endDate.Add(-requestedTimespanLaunchpad)
				// Stop when reaching 2000-01-01T00:00:00Z
				lastTimespan = startDate.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
				requestUrl = fmt.Sprintf("https://api.launchpad.net/devel/people?created_after=\"%s\"&created_before=\"%s\"&text=\"\"&ws.op=findPerson&ws.size=100",
					url.QueryEscape(startDate.UTC().Format("2006-01-02T15:04:05Z")),
					url.QueryEscape(endDate.UTC().Format("2006-01-02T15:04:05Z")))
			}
		}
		var res *http.Response
		var err error
		for res == nil || err != nil {
			res, err = httpClient.Get(requestUrl)
			if err != nil || res.Header.Get("Content-Type") != "application/json" {
				// Any non-JSON response is most likely caused by timeouts on Launchpad's side
				// We therefore retry the request a few times before giving up
				res = nil
				retry++
				if retry <= maxRetries {
					s.log("failed to retrieve users from launchpad at cursor %v (page %v), retrying %v/%v", true, s.Cursor, page, retry, maxRetries)
					continue
				}
				// If we encounter any error after exceeding maxRetries, we wait for the configured duration before continuing
				s.log("exceeded max retries while retrieving users at cursor %v (page %v), giving up for now", true, s.Cursor, page)
				s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
				return false, err
			}
		}
		if retry > 0 {
			s.log("retried %v time(s) before request succeeded", false, retry)
			retry = 0
		}

		if res.StatusCode != 200 {
			if res.StatusCode == 429 {
				// Launchpad does not have a publicly documented rate limit, but we pay respect to the status code anyway
				// If we hit the rate limit, we wait for the rate limit reset time before continuing
				s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
				s.log("hit rate limit, continuing at %v", true, s.ContinueAt)
				return false, fmt.Errorf("rate limit hit")
			}
			// If we encounter any unknown error, we wait for the configured duration before continuing
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			return false, fmt.Errorf("unexpected status code while retrieving users at cursor %v (url: %v): %v", s.Cursor, requestUrl, res.StatusCode)
		}
		var usersResponse LaunchpadPeopleApiResponse
		if err := json.NewDecoder(res.Body).Decode(&usersResponse); err != nil {
			s.log("failed to decode people api response from json at cursor %v (url: %v): %v", true, s.Cursor, requestUrl, err)
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			return false, err
		}

		users := make(chan LaunchpadPeopleApiEntry, 100)
		failures := make(chan error, concurrentRequests)
		wg.Add(concurrentRequests)
		for i := 0; i < concurrentRequests; i++ {
			go s.publicKeyWorker(ctx, users, failures, &wg)
		}
		for _, user := range usersResponse.Entries {
			users <- user
		}
		close(users)
		// Wait for processing to complete
		wg.Wait()

		if len(failures) > 0 {
			// If we encountered any errors while processing users, we return the first error
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigApiErrorCooldown))
			return false, <-failures
		}

		if usersResponse.NextCollectionLink == nil {
			// If there is no next collection link, we have processed all users in the current timespan
			s.updateCursor(ctx)
			page = 0
			if lastTimespan {
				// If we have processed all timespans, we finish the scrape
				return true, nil
			}
		} else {
			// If there is a next collection link, we continue with the next page in the next iteration
			requestUrl = *usersResponse.NextCollectionLink
			page++
		}

		iterationEnd := time.Now()
		iterationDuration := iterationEnd.Sub(iterationStart)
		if iterationDuration < minimumIterationDuration {
			time.Sleep(minimumIterationDuration - iterationDuration)
		}
	}
}
