// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
	r "github.com/dancannon/gorethink"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var session *r.Session

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel     = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	hostIdentifier = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	skipEmptyFiles = flag.Bool("skip-empty-files", false, "Skip writting empty files")
	topicPollRate  = flag.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")

	consumerOpts     = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
	topics           = util.StringArray{}
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
}

type Logger struct {
	out      *os.File
	logChan  chan *nsq.Message
	ExitChan chan int
	termChan chan bool
	hupChan  chan bool
}

type ConsumerLogger struct {
	F *Logger
	C *nsq.Consumer
}

type TopicDiscoverer struct {
	topics   map[string]*ConsumerLogger
	termChan chan os.Signal
	hupChan  chan os.Signal
	wg       sync.WaitGroup
}

func newTopicDiscoverer() *TopicDiscoverer {
	return &TopicDiscoverer{
		topics:   make(map[string]*ConsumerLogger),
		termChan: make(chan os.Signal),
		hupChan:  make(chan os.Signal),
	}
}

func (l *Logger) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	l.logChan <- m
	return nil
}

func (logger *Logger) router(r *nsq.Consumer) {
	pos := 0
	output := make([]*nsq.Message, *maxInFlight)
	sync := false
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	closing := false
	exit := false

	for {
		select {
		case <-r.StopChan:
			sync = true
			exit = true
		case <-logger.termChan:
			ticker.Stop()
			r.Stop()
			sync = true
			closing = true
		case <-logger.hupChan:
			sync = true
		case m := <-logger.logChan:
			record := ApiCall{string(m.Body[:])}
			_, err := insertRecord(record)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err)
			}
			output[pos] = m
			pos++
			if pos == cap(output) {
				sync = true
			}
		}

		if closing || sync || r.IsStarved() {
			if pos > 0 {
				log.Printf("syncing %d records to disk ----------------", pos)
				for pos > 0 {
					pos--
					m := output[pos]
					m.Finish()
					output[pos] = nil
				}
			}
			sync = false
		}

		if exit {
			close(logger.ExitChan)
			cleanup()
			break
		}
	}
}

func NewLogger(topic string) (*Logger, error) {

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	shortHostname := strings.Split(hostname, ".")[0]
	identifier := shortHostname
	if len(*hostIdentifier) != 0 {
		identifier = strings.Replace(*hostIdentifier, "<SHORT_HOST>", shortHostname, -1)
		identifier = strings.Replace(identifier, "<HOSTNAME>", hostname, -1)
	}

	f := &Logger{
		logChan:  make(chan *nsq.Message, 1),
		ExitChan: make(chan int),
		termChan: make(chan bool),
		hupChan:  make(chan bool),
	}
	return f, nil
}

func hasArg(s string) bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, s) {
			return true
		}
	}
	return false
}

func newConsumerLogger(topic string) (*ConsumerLogger, error) {
	logger, err := NewLogger(topic)
	if err != nil {
		return nil, err
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsq_to_rethinkdb/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)
	err = util.ParseOpts(cfg, consumerOpts)
	if err != nil {
		return nil, err
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(topic, *channel, cfg)
	if err != nil {
		return nil, err
	}

	consumer.AddHandler(logger)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	return &ConsumerLogger{
		C: consumer,
		F: logger,
	}, nil
}

func (t *TopicDiscoverer) startTopicRouter(logger *ConsumerLogger) {
	t.wg.Add(1)
	defer t.wg.Done()
	go logger.F.router(logger.C)
	<-logger.F.ExitChan
}

func (t *TopicDiscoverer) syncTopics(addrs []string) {
	newTopics, err := lookupd.GetLookupdTopics(addrs)
	if err != nil {
		log.Printf("ERROR: could not retrieve topic list: %s", err)
	}
	for _, topic := range newTopics {
		if _, ok := t.topics[topic]; !ok {
			logger, err := newConsumerLogger(topic)
			if err != nil {
				log.Printf("ERROR: couldn't create logger for new topic %s: %s", topic, err)
				continue
			}
			t.topics[topic] = logger
			go t.startTopicRouter(logger)
		}
	}
}

func (t *TopicDiscoverer) stop() {
	for _, topic := range t.topics {
		topic.F.termChan <- true
	}
}

func (t *TopicDiscoverer) hup() {
	for _, topic := range t.topics {
		topic.F.hupChan <- true
	}
}

func (t *TopicDiscoverer) watch(addrs []string, sync bool) {
	ticker := time.Tick(*topicPollRate)
	for {
		select {
		case <-ticker:
			if sync {
				t.syncTopics(addrs)
			}
		case <-t.termChan:
			t.stop()
			t.wg.Wait()
			return
		case <-t.hupChan:
			t.hup()
		}
	}
}

func initdb() (*r.Session, error) {

	return r.Connect(r.ConnectOpts{
		Address:     "localhost:28015",
		Database:    "logs",
		MaxIdle:     10,
		IdleTimeout: time.Second * 10,
	})
}

type ApiCall struct {
	Body string `gorethink:"body"`
	// Url     string `gorethink:"url"`
	// Method  string `gorethink:"method"`
	// Headers string `gorethink:"headers"`
}

func insertRecord(record ApiCall) (ApiCall, error) {

	_, err := r.Table("api_calls").Insert(record).Run(session)

	return record, err
}

func cleanup() {
	log.Printf("cleaning up............")
	session.Close()
	log.Printf("rethink db session closed.")
}

func main() {
	var err error
	session, err = initdb()
	if err != nil {
		log.Fatalln(err.Error())
	} else {
		log.Printf("connected to rethink..............")
	}

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", util.BINARY_VERSION)
		return
	}

	if *channel == "" {
		log.Fatal("--channel is required")
	}

	var topicsFromNSQLookupd bool

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	discoverer := newTopicDiscoverer()

	signal.Notify(discoverer.hupChan, syscall.SIGHUP)
	signal.Notify(discoverer.termChan, syscall.SIGINT, syscall.SIGTERM)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		cleanup()
	}()

	if len(topics) < 1 {
		if len(lookupdHTTPAddrs) < 1 {
			log.Fatal("use --topic to list at least one topic to subscribe to or specify at least one --lookupd-http-address to subscribe to all its topics")
		}
		topicsFromNSQLookupd = true
		var err error
		topics, err = lookupd.GetLookupdTopics(lookupdHTTPAddrs)
		if err != nil {
			log.Fatalf("ERROR: could not retrieve topic list: %s", err)
		}
	}

	for _, topic := range topics {
		logger, err := newConsumerLogger(topic)
		if err != nil {
			log.Fatalf("ERROR: couldn't create logger for topic %s: %s", topic, err)
		}
		discoverer.topics[topic] = logger
		go discoverer.startTopicRouter(logger)
	}

	discoverer.watch(lookupdHTTPAddrs, topicsFromNSQLookupd)
}
