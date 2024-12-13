package main

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/viper"
	"log"
	"os"
)

func loadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("[!] unable to read config file: %s", err.Error())
	}
}

func initScheduler() (quartz.Scheduler, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sched := quartz.NewStdScheduler()
	sched.Start(ctx)
	return sched, ctx, cancel
}

func initElasticsearch() *elasticsearch.TypedClient {
	var es *elasticsearch.TypedClient
	var err error
	if viper.GetBool("elasticsearch.tls.enabled") {
		var caCert []byte
		caCert, err = os.ReadFile(viper.GetString("elasticsearch.tls.caCert"))
		if err != nil {
			log.Fatalf("[!] failed to read elasticsearch CA cert: %s", err.Error())
		}
		es, err = elasticsearch.NewTypedClient(elasticsearch.Config{
			Addresses:     viper.GetStringSlice("elasticsearch.url"),
			Username:      viper.GetString("elasticsearch.username"),
			Password:      viper.GetString("elasticsearch.password"),
			CACert:        caCert,
			EnableMetrics: false,
		})
	} else {
		es, err = elasticsearch.NewTypedClient(elasticsearch.Config{
			Addresses:     viper.GetStringSlice("elasticsearch.url"),
			Username:      viper.GetString("elasticsearch.username"),
			Password:      viper.GetString("elasticsearch.password"),
			EnableMetrics: false,
		})
	}
	if err != nil {
		log.Fatalf("[!] failed to create elasticsearch client: %s", err.Error())
	}
	return es
}

func main() {
	loadConfig()
	sched, ctx, cancel := initScheduler()
	defer cancel()
	es := initElasticsearch()

	// GitHub
	if viper.GetBool("scrapers.github.enabled") {
		scraper, err := LoadScraper(ctx, es, Github)
		if err != nil {
			log.Fatalf("[!] failed to load github scraper from database: %s", err.Error())
		}
		scraper.Register(sched)
	}

	// GitLab
	if viper.GetBool("scrapers.gitlab.enabled") {
		scraper, err := LoadScraper(ctx, es, Gitlab)
		if err != nil {
			log.Fatalf("[!] failed to load gitlab scraper from database: %s", err.Error())
		}
		scraper.Register(sched)
	}

	// Launchpad
	if viper.GetBool("scrapers.launchpad.enabled") {
		scraper, err := LoadScraper(ctx, es, Launchpad)
		if err != nil {
			log.Fatalf("[!] failed to load launchpad scraper from database: %s", err.Error())
		}
		scraper.Register(sched)
	}

	<-ctx.Done()
}
