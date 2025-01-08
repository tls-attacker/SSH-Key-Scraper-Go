package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reugn/go-quartz/job"
	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/viper"
	"log"
	"os"
	"time"
)

type Platform string

const (
	Github    Platform = "github"
	Gitlab    Platform = "gitlab"
	Launchpad Platform = "launchpad"
)

type JobType string

const (
	FullJob        JobType = "full"
	IncrementalJob JobType = "incremental"
	ResetJob       JobType = "reset"
)

type PlatformConfigKey string

const (
	ConfigReverse                    PlatformConfigKey = "reverse"
	ConfigToken                      PlatformConfigKey = "token"
	ConfigTimeout                    PlatformConfigKey = "timeout"
	ConfigMaxRetries                 PlatformConfigKey = "maxRetries"
	ConfigConcurrentRequests         PlatformConfigKey = "concurrentRequests"
	ConfigSecondaryRateLimitCooldown PlatformConfigKey = "secondaryRateLimitCooldown"
	ConfigApiErrorCooldown           PlatformConfigKey = "apiErrorCooldown"
	ConfigMinimumIterationDuration   PlatformConfigKey = "minimumIterationDuration"
	ConfigUserIndex                  PlatformConfigKey = "userIndex"
	ConfigIncrementalInterval        PlatformConfigKey = "incrementalInterval"
	ConfigFullInterval               PlatformConfigKey = "fullInterval"
)

type Scraper struct {
	databaseID     string
	Platform       Platform  `json:"platform"`
	ContinueAt     time.Time `json:"continueAt"`
	RescrapeAt     time.Time `json:"rescrapeAt"`
	FullScrapeDone bool      `json:"fullScrapeDone"`
	Cursor         string    `json:"cursor"`
	UserIndex      string    `json:"userIndex"`

	Elasticsearch *elasticsearch.TypedClient `json:"-"`

	context   context.Context
	scheduler quartz.Scheduler
}

type UserEntry struct {
	Username   string           `json:"username"`
	PublicKeys []PublicKeyEntry `json:"publicKeys"`
	Metadata   map[string]any   `json:"metadata"`
	VisitedAt  time.Time        `json:"visitedAt"`
	Deleted    bool             `json:"deleted"`
}

type PublicKeyEntry struct {
	Key       string         `json:"key"`
	Metadata  map[string]any `json:"metadata"`
	VisitedAt time.Time      `json:"visitedAt"`
	Deleted   bool           `json:"deleted"`
}

func (s *Scraper) ScrapePlatform(ctx context.Context) (bool, error) {
	switch s.Platform {
	case Github:
		scraper := &GithubScraper{
			s,
		}
		if err := s.createUserIndex(ctx,
			scraper.getUserMetadataMapping(),
			scraper.getPublicKeyMetadataMapping()); err != nil {
			panic(err)
		}
		return scraper.Scrape(ctx)
	case Gitlab:
		scraper := &GitlabScraper{
			s,
		}
		if err := s.createUserIndex(ctx,
			scraper.getUserMetadataMapping(),
			scraper.getPublicKeyMetadataMapping()); err != nil {
			panic(err)
		}
		return scraper.Scrape(ctx)
	case Launchpad:
		scraper := &LaunchpadScraper{
			s,
		}
		if err := s.createUserIndex(ctx,
			scraper.getUserMetadataMapping(),
			scraper.getPublicKeyMetadataMapping()); err != nil {
			panic(err)
		}
		return scraper.Scrape(ctx)
	default:
		return false, fmt.Errorf("scraper for platform not yet implemented")
	}
}

func (s *Scraper) saveUnprocessedUser(key string, user any) error {
	dir := fmt.Sprintf("unprocessed_%s", s.Platform)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for unprocessed user files: %w", err)
		}
	}
	file, err := os.Create(fmt.Sprintf("%s/%s.json", dir, key))
	if err != nil {
		return fmt.Errorf("failed to save unprocessed user to file: %w", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(user); err != nil {
		return fmt.Errorf("failed to encode unprocessed user to json: %w", err)
	}
	return nil
}

func (s *Scraper) getPlatformConfigBool(key PlatformConfigKey) bool {
	return viper.GetBool(fmt.Sprintf("scrapers.%s.%s", s.Platform, key))
}

func (s *Scraper) getPlatformConfigString(key PlatformConfigKey) string {
	return viper.GetString(fmt.Sprintf("scrapers.%s.%s", s.Platform, key))
}

func (s *Scraper) getPlatformConfigInt(key PlatformConfigKey) int {
	return viper.GetInt(fmt.Sprintf("scrapers.%s.%s", s.Platform, key))
}

func (s *Scraper) getPlatformConfigDuration(key PlatformConfigKey) time.Duration {
	return viper.GetDuration(fmt.Sprintf("scrapers.%s.%s", s.Platform, key))
}

func (s *Scraper) log(format string, error bool, v ...any) {
	if error {
		log.Printf(fmt.Sprintf("[!][%v] %s", s.Platform, format), v...)
	} else {
		log.Printf(fmt.Sprintf("[i][%v] %s", s.Platform, format), v...)
	}
}

func (s *Scraper) createUserIndex(ctx context.Context, userMeta *types.ObjectProperty, publicKeyMeta *types.ObjectProperty) error {
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
					"username": types.NewKeywordProperty(),
					"publicKeys": &types.NestedProperty{
						Properties: map[string]types.Property{
							"key":       types.NewTextProperty(),
							"metadata":  publicKeyMeta,
							"visitedAt": types.NewDateProperty(),
							"deleted":   types.NewBooleanProperty(),
						},
					},
					"metadata":  userMeta,
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

func (s *Scraper) getFullScrapeJob() quartz.Job {
	return job.NewFunctionJobWithDesc(func(ctx context.Context) (interface{}, error) {
		done, err := s.ScrapePlatform(ctx)
		if err != nil {
			s.log("error occured during scrape run: %v", true, err)
		} else if done {
			s.log("full scrape done, scheduling reset and incremental jobs", false)
			s.FullScrapeDone = true
			s.RescrapeAt = time.Now().Add(s.getPlatformConfigDuration(ConfigFullInterval))
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigIncrementalInterval))
			s.scheduleResetJob()
		} else {
			s.log("full scrape not complete, rescheduling job at %v", false, s.ContinueAt.Format(time.RFC3339))
		}
		// Schedule next scrape job
		s.scheduleScrapeJob()
		return nil, s.Save(ctx)
	}, fmt.Sprintf("[%v] Scrapes all SSH keys from all users", s.Platform))
}

func (s *Scraper) getIncrementalScrapeJob() quartz.Job {
	return job.NewFunctionJobWithDesc(func(ctx context.Context) (interface{}, error) {
		done, err := s.ScrapePlatform(ctx)
		if err != nil {
			s.log("error occured during scrape run: %v", true, err)
		} else if done {
			s.ContinueAt = time.Now().Add(s.getPlatformConfigDuration(ConfigIncrementalInterval))
			s.log("incremental scrape done, rescheduling incremental scrape job at %v", false, s.ContinueAt.Format(time.RFC3339))
		} else {
			s.log("incremental scrape not complete, rescheduling job at %v", false, s.ContinueAt.Format(time.RFC3339))
		}
		// Schedule next scrape job
		s.scheduleScrapeJob()
		return nil, s.Save(ctx)
	}, fmt.Sprintf("[%v] Scrapes SSH keys from new users since last execution", s.Platform))
}

func (s *Scraper) scheduleScrapeJob() {
	var scrapeJob quartz.Job
	var jobKey *quartz.JobKey
	var trigger quartz.Trigger
	immediate := false
	// Check if API rate limit reset is in the future
	if s.ContinueAt.Compare(time.Now()) > 0 {
		// Add 1 minute to the rate limit reset time to ensure the rate limit is reset
		trigger = quartz.NewRunOnceTrigger(s.ContinueAt.Sub(time.Now()))
	} else {
		immediate = true
		trigger = quartz.NewRunOnceTrigger(0)
	}
	if s.FullScrapeDone {
		scrapeJob = s.getIncrementalScrapeJob()
		jobKey = s.GetJobKey(IncrementalJob)
		if immediate {
			s.log("starting incremental scrape job immediately", false)
		} else {
			s.log("scheduling incremental scrape job at %v", false, s.ContinueAt.Format(time.RFC3339))
		}
	} else {
		scrapeJob = s.getFullScrapeJob()
		jobKey = s.GetJobKey(FullJob)
		if immediate {
			s.log("starting full scrape job immediately", false)
		} else {
			s.log("scheduling full scrape job at %v", false, s.ContinueAt.Format(time.RFC3339))
		}
	}
	err := s.scheduler.ScheduleJob(quartz.NewJobDetail(scrapeJob, jobKey), trigger)
	if err != nil {
		log.Fatalf("[!][%v] failed to schedule scrape job: %v", s.Platform, err)
	}
}

func (s *Scraper) getResetJob() quartz.Job {
	return job.NewFunctionJobWithDesc(func(ctx context.Context) (interface{}, error) {
		// This should always be true since the job is only scheduled once
		// the last full scrape is done
		if s.FullScrapeDone {
			s.FullScrapeDone = false
			s.Cursor = ""
			// Cancel the incremental scrape job
			incrementalJobKey := s.GetJobKey(IncrementalJob)
			err := s.scheduler.DeleteJob(incrementalJobKey)
			if err != nil {
				s.log("failed to delete incremental scrape job: %v", true, err)
				panic(err)
			}
			// Schedule full scrape job
			s.scheduleScrapeJob()
		}
		return nil, s.Save(ctx)
	}, fmt.Sprintf("[%v] Resets the scraper to detect changes to existing accounts and / or SSH public keys by scheduling a full scrape job", s.Platform))
}

func (s *Scraper) scheduleResetJob() {
	if s.FullScrapeDone {
		s.log("full scrape completed, scheduling reset job at %v", false, s.RescrapeAt.Format(time.RFC3339))
		err := s.scheduler.ScheduleJob(quartz.NewJobDetail(
			s.getResetJob(),
			s.GetJobKey(ResetJob)),
			quartz.NewRunOnceTrigger(s.RescrapeAt.Sub(time.Now())))
		if err != nil {
			s.log("failed to schedule reset job: %v", true, err)
			panic(err)
		}
	} else {
		s.log("tried to schedule reset job before full scrape completed", true)
	}
}

func LoadScraper(ctx context.Context, es *elasticsearch.TypedClient, platform Platform) (*Scraper, error) {
	scraperIndex := viper.GetString("scraperIndex")
	if exists, err := es.Indices.Exists(scraperIndex).Do(ctx); !exists {
		_, err := es.Indices.Create(scraperIndex).Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"platform":                 types.NewKeywordProperty(),
					"continueAt":               types.NewDateProperty(),
					"rescrapeAt":               types.NewDateProperty(),
					"fullScrapeDone":           types.NewBooleanProperty(),
					"cursor":                   types.NewTextProperty(),
					"userIndex":                types.NewKeywordProperty(),
					"minimumIterationDuration": types.NewLongNumberProperty(),
				},
			},
			Settings: &types.IndexSettings{
				NumberOfShards:   "1",
				NumberOfReplicas: "2",
			},
		}).Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create scraper index: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to check if scraper index exists: %w", err)
	}
	res, err := es.Search().
		Index(scraperIndex).
		Request(&search.Request{
			Query: &types.Query{
				Match: map[string]types.MatchQuery{
					"platform": {Query: string(platform)},
				},
			},
		}).Preference("_local").Do(ctx)
	var scraper *Scraper
	if err != nil {
		return nil, fmt.Errorf("failed to check if scraper exists in database: %w", err)
	} else if res.Hits.Total.Value == 0 {
		scraper = &Scraper{
			Platform:       platform,
			RescrapeAt:     time.Now().Add(30 * 24 * time.Hour),
			FullScrapeDone: false,
			Elasticsearch:  es,
			context:        ctx,
		}
		scraper.UserIndex = scraper.getPlatformConfigString(ConfigUserIndex)
		err = scraper.Save(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create new scraper: %w", err)
		}
	} else {
		if err = json.Unmarshal(res.Hits.Hits[0].Source_, &scraper); err != nil {
			return nil, fmt.Errorf("failed to unmarshal github scraper: %w", err)
		}
		scraper.databaseID = *res.Hits.Hits[0].Id_
		scraper.Elasticsearch = es
		scraper.context = ctx
	}
	return scraper, nil
}

func (s *Scraper) Save(ctx context.Context) error {
	scraperIndex := viper.GetString("scraperIndex")
	if s.databaseID != "" {
		_, err := s.Elasticsearch.Index(scraperIndex).Id(s.databaseID).Request(s).Do(ctx)
		if err != nil {
			return fmt.Errorf("failed to update scraper for platform '%v': %w", s.Platform, err)
		}
		return nil
	} else {
		res, err := s.Elasticsearch.Index(scraperIndex).Request(s).Do(ctx)
		if err != nil {
			return fmt.Errorf("failed to insert scraper for platform '%v': %w", s.Platform, err)
		}
		s.databaseID = res.Id_
		return nil
	}
}

func (s *Scraper) Register(scheduler quartz.Scheduler) {
	s.scheduler = scheduler
	if s.FullScrapeDone {
		s.scheduleResetJob()
	}
	s.scheduleScrapeJob()
}

func (s *Scraper) GetJobKey(jobType JobType) *quartz.JobKey {
	return quartz.NewJobKeyWithGroup(string(jobType), string(s.Platform))
}
