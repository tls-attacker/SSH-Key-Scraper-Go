package scrapers

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
	"log"
	"time"
)

const ScraperIndex = "sshks_scrapers"

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

type Scraper struct {
	databaseID     string
	Platform       Platform  `json:"platform"`
	ContinueAt     time.Time `json:"continueAt"`
	RescrapeAt     time.Time `json:"rescrapeAt"`
	FullScrapeDone bool      `json:"fullScrapeDone"`
	Cursor         string    `json:"cursor"`

	Elasticsearch *elasticsearch.TypedClient `json:"-"`

	context   context.Context
	scheduler quartz.Scheduler
}

func (s *Scraper) scrape(ctx context.Context) (bool, error) {
	switch s.Platform {
	case Github:
		return ScrapePlatform(ctx, s)
	default:
		return false, fmt.Errorf("[!][%v] scraper for platform not yet implemented")
	}
}

func (s *Scraper) getFullScrapeJob() quartz.Job {
	return job.NewFunctionJobWithDesc(func(ctx context.Context) (interface{}, error) {
		done, err := s.scrape(ctx)
		if err != nil {
			log.Printf("[!][%v] error occured during scrape run: %v", s.Platform, err)
		} else if done {
			log.Printf("[i][%v] full scrape done, scheduling reset and incremental jobs", s.Platform)
			s.FullScrapeDone = true
			// Next full scrape in 30 days
			s.RescrapeAt = time.Now().Add(30 * 24 * time.Hour)
			// Next incremental scrape in 24 hours
			s.ContinueAt = time.Now().Add(24 * time.Hour)
			s.scheduleResetJob()
		} else {
			log.Printf("[i][%v] full scrape not complete, rescheduling job at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
		}
		// Schedule next scrape job
		s.scheduleScrapeJob()
		return nil, s.Save(ctx)
	}, fmt.Sprintf("[%v] Scrapes all SSH keys from all users", s.Platform))
}

func (s *Scraper) getIncrementalScrapeJob() quartz.Job {
	return job.NewFunctionJobWithDesc(func(ctx context.Context) (interface{}, error) {
		_, err := s.scrape(ctx)
		if err != nil {
			log.Printf("[!][%v] error occured during scrape run: %v", s.Platform, err)
		} else {
			log.Printf("[i][%v] incremental scrape done, rescheduling incremental scrape job at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
		}
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
			log.Printf("[i][%v] starting incremental scrape job immediately", s.Platform)
		} else {
			log.Printf("[i][%v] scheduling incremental scrape job at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
		}
	} else {
		scrapeJob = s.getFullScrapeJob()
		jobKey = s.GetJobKey(FullJob)
		if immediate {
			log.Printf("[i][%v] starting full scrape job immediately", s.Platform)
		} else {
			log.Printf("[i][%v] scheduling full scrape job at %v", s.Platform, s.ContinueAt.Format(time.RFC3339))
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
				log.Fatalf("[!][%v] failed to delete incremental scrape job: %v", s.Platform, err)
			}
			// Schedule full scrape job
			s.scheduleScrapeJob()
		}
		return nil, s.Save(ctx)
	}, fmt.Sprintf("[%v] Resets the scraper to detect changes to existing accounts and / or SSH public keys by scheduling a full scrape job", s.Platform))
}

func (s *Scraper) scheduleResetJob() {
	if s.FullScrapeDone {
		log.Printf("[i][%v] full scrape completed, scheduling reset job at %v", s.Platform, s.RescrapeAt.Format(time.RFC3339))
		err := s.scheduler.ScheduleJob(quartz.NewJobDetail(
			s.getResetJob(),
			s.GetJobKey(ResetJob)),
			quartz.NewRunOnceTrigger(s.RescrapeAt.Sub(time.Now())))
		if err != nil {
			log.Fatalf("[!][%v] failed to schedule reset job: %v", s.Platform, err)
		}
	} else {
		log.Printf("[~][%v] tried to schedule reset job before full scrape completed", s.Platform)
	}
}

func LoadScraper(ctx context.Context, es *elasticsearch.TypedClient, platform Platform) (*Scraper, error) {
	if exists, err := es.Indices.Exists(ScraperIndex).Do(ctx); !exists {
		_, err := es.Indices.Create(ScraperIndex).Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"platform":       types.NewKeywordProperty(),
					"continueAt":     types.NewDateProperty(),
					"rescrapeAt":     types.NewDateProperty(),
					"fullScrapeDone": types.NewBooleanProperty(),
					"cursor":         types.NewTextProperty(),
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
		Index(ScraperIndex).
		Request(&search.Request{
			Query: &types.Query{
				Match: map[string]types.MatchQuery{
					"platform": {Query: string(platform)},
				},
			},
		}).Do(ctx)
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
	if s.databaseID != "" {
		_, err := s.Elasticsearch.Index(ScraperIndex).Id(s.databaseID).Request(s).Do(ctx)
		if err != nil {
			return fmt.Errorf("failed to update scraper for platform '%v': %w", s.Platform, err)
		}
		return nil
	} else {
		res, err := s.Elasticsearch.Index(ScraperIndex).Request(s).Do(ctx)
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
