package app

import (
	"context"
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/wb-client/cards"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/cron"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/pkg/local_clients/balance"

	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/pkg/local_clients/http_client"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/pkg/local_clients/raiting"
	"net/http"
	"slices"
	"strings"
	"time"

	returned_boxes "gitlab.wildberries.ru/ext-delivery/ext-delivery/returned-boxes.git/pkg/api/returned"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/wb-client/kafkasub"
	api_shard2 "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/client/api_shard"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/client/backoffice_api"
	api_shard "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/client/bert_api"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/client/hr_employees"

	garyRedisPool "github.com/garyburd/redigo/redis"
	goRedis "github.com/go-redis/redis"
	"github.com/gomodule/redigo/redis"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/segmentio/kafka-go"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/api/pkg/api/backoffice"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/api/pkg/api/external"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/gopkg-app-core/app"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/gopkg-app-core/middleware"
	errors "gitlab.wildberries.ru/ext-delivery/ext-delivery/gopkg-errors"
	logger "gitlab.wildberries.ru/ext-delivery/ext-delivery/gopkg-logger"
	chunks "gitlab.wildberries.ru/ext-delivery/ext-delivery/wb-client/chunks/redischunks"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/wb-client/storage"
	"gitlab.wildberries.ru/ext-delivery/ext-delivery/wb-client/userinfo"
	adminapi "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/api/admin"
	externalapi "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/api/external_api"
	publicapi "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/api/public"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/cache"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/model"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/pgxRepository"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/service"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/config"
	pkgmw "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/middleware"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/session"
	sessmw "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/pkg/session/middleware"
	write_off "gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/pkg/local_clients/write-off/gitlab.wildberries.ru/ext-delivery/ext-delivery/write-off"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var (
	// override by ldflags
	Name    = "tickets"
	Version = "DEV"
)

const (
	internalKey      = "internal-pp-id"
	externalKey      = "external-pp-id"
	internalPpHeader = "X-Pickpoint-Id"
	externalPpHeader = "X-Pickpoint-External-Id"
	ServiceName      = "tickets"
	stageEnv         = "stage"
	prdEnv           = "prod"
)

type CustomApp struct {
	coreApp               *app.App
	publicAPI             *publicapi.Implementation
	adminApi              *adminapi.Implementation
	externalApi           *externalapi.Implementation
	TicketsKafkaProcessor []IKafkaProcessor
}

func Run(ctx context.Context) error {
	flag.BoolVar(&logger.Jsonlog, "jsonlog", false, "Enable logging in JSON format")
	flag.BoolVar(&logger.Verbose, "verbose", false, "Enable verbose output")
	flag.Parse()

	cfg := config.Load()

	a, err := buildApp(ctx, cfg)
	if err != nil {
		return err
	}
	for _, proc := range a.TicketsKafkaProcessor {
		go proc.RunLimitManualCommit(ctx)
	}
	a.coreApp.Run(a.publicAPI, a.adminApi, a.externalApi)

	return nil
}

type builder struct {
	customApp *CustomApp

	publicCloser app.PublicCloserFnMap
}

func buildApp(ctx context.Context, cfg config.Config) (*CustomApp, error) {
	b := &builder{
		customApp:    &CustomApp{},
		publicCloser: make(app.PublicCloserFnMap, 0),
	}

	unaryInterceptor, publicMiddleware, err := b.initDeps(ctx, cfg)
	if err != nil {
		return nil, err
	}

	a, err := b.initApp(Name, Version, unaryInterceptor, publicMiddleware, cfg)
	if err != nil {
		return nil, err
	}

	b.customApp.coreApp = a
	return b.customApp, nil
}

func (b *builder) initApp(name, version string, unaryInterceptor []grpc.UnaryServerInterceptor, publicMiddleware []func(http.Handler) http.Handler, cfg config.Config) (*app.App, error) {
	favicon, _ := base64.StdEncoding.DecodeString("AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAACnEMRgphG/36MRt/6eEK7/mxGm/5gRnv+UEZb/kRGN/o0Rhf+KEX3/hhF1/4IRbf+AEWX+fBFd/3gQVe52EE5fqhHH76YRv/+iEbb/nxGv/5sRpv+YEZ7/lBGW/5EQjf6NEYb/ihF9/4YRdf+CEW3/fxFl/3sRXP94EVT/dRBN3qkRx/+mEb//ohG3/58Rr/+cEaf/lxGe/5QRlv+QEY7/jRGF/4kRff+GEXX+ghFt/38RZf97EVz/eBFV/3QRTP+pEcf/phG//6IQt/6fEa/+mxGm/5gRnv+VEJb+kRCN/o4Shv+JEX3/hhF1/4MRbf+AEWX/exFc/3gRVP51EUz/qRHH/6URvv+iEbb/nhGu/+bD6f//////r0yw/5ERjf6NEYX/p0yf///////n0uP/gBFl/3sRXP94EVT/dBFM/6oQx/6lEb//ohK3/6UgtP///////////9am1/+REY7/jRGF/8yXx////////////48vd/97EVz/eBFV/3QRTP+qEcf+phG//qIRtv/JedL//////9OX1v7x4fH/kRGN/owQhP738Pf/ypbB//////+vap3/fBFd/3gRVP90EEz+qRHH/6URvv+iEbb/4bTm//Lh8/+lL6v//////6xMqf+qTaX//////5Uvhv7w4e3/17XO/3wRXP94EVT/dRFM/6kRx/+mEb/+pyC7//nw+//Tl9f/mBGe/+vS6//Ol8z/zZfK/+HD3v+FEHX+0aXJ//fw9f97EFz+eBFU/3QRTP+pEcb/phG//7pNyf//////tUy8/pgQn/7Rl9L/8eHw//Hh8P/EiL3/hhF1/6JMkf//////nU2F/3gQVP51EUz/qRHH/6YRv//Xl+D/+fD6/6EgrP+XEZ7/qT6q////////////pkyd/4UQdf6CEWz/9/D1/82mwv94EFT+dRFM/6oRyP6lEb7/9OH3/+fE6v+bEab/lxGe/5QRlv/y4fL/8eHv/4oRff+GEXb/ghBt/te0z//u4er/eBFU/nURTP+pEcf/phK//6IRtv+fEa7/mxGm/5gRnv+UEZb/kRGO/40Rhf+JEH3+hhF1/4IRbf+AEWX/fBFd/3gRVP90EUz/qhDH/qYQv/6iEbf/nxCv/psRpv+YEZ7/lBGW/5ERjf6OEob/ihB9/oUQdf6CEW3+gBFl/nwRXP94EFT+dRBM/qkRxt+mEL/+ohG2/58Rr/6bEab/mBGe/5URl/6QEY3/jRGG/4oRff+GEXX/ghFt/4ARZP57EFz/eBFU/3URTO6pEMdfphHA7qIRtv+fEK7+mxGm/5cRnf+VEJb+kBGN/40Rhf6KEX3/hhF1/4IRbP+AEWT+fBFc/3gRVd92EE5fgAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAEAAA==")
	opts := []app.OptionFn{
		app.WithSwaggerOption(pkgmw.NewAuthSwaggerOption()),
		app.WithUnaryPrependInterceptor(unaryInterceptor...),
		app.WithPublicMiddleware(publicMiddleware...),
		app.WithFavicon(favicon),
		app.WithPprof(true),
		app.WithPublicCloser(b.publicCloser),
	}

	return app.NewApp(
		app.Config{
			Name:              name,
			Version:           version,
			Host:              cfg.Host,
			LogIgnoreHandlers: []string{"/health", "/tickets.public.PublicAPI/Health"},
			JwtKey:            cfg.JwtKey,
			RsaPublicKey:      cfg.Secrets.RsaPublicKey,
			Listener: app.ConfigListener{
				Host:          cfg.Host,
				HttpPort:      cfg.HTTPPort,
				GrpcPort:      cfg.GRPCPort,
				HttpAdminPort: cfg.AdminPort,
			},
		},
		opts...,
	)
}

func (b *builder) initDeps(ctx context.Context, cfg config.Config) ([]grpc.UnaryServerInterceptor, []func(http.Handler) http.Handler, error) {

	redisPool := &redis.Pool{
		Wait:      true,
		MaxActive: cfg.RedisMaxActive,
		MaxIdle:   cfg.RedisMaxIdle,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.DialURL(
				cfg.RedisURL,
			)
			return conn, err
		},
	}

	sessionClient, closeFn, err := session.NewSession(redisPool)
	if err != nil {
		return nil, nil, err
	}

	b.publicCloser.Add("redis.session", func() error {
		app.GracefulDelay("redis.session")

		if err := closeFn(); err != nil {
			return errors.Internal.Err("redis.session: error during shutdown").
				WithLogKV("error", err)
		}
		logger.Info(logger.App, "redis.session: gracefully stopped")
		return nil
	})

	publicMiddleware := []func(http.Handler) http.Handler{
		middleware.NewAddAcceptMiddleware("application/json", []string{"/*"}),
		sessmw.NewSessionServerMiddleware(sessionClient),
	}

	unaryInterceptor := []grpc.UnaryServerInterceptor{
		NewAuthServerInterceptor(cfg),
	}

	// bind requestId to logger
	var getRequestIDFromContextFn logger.GetRequestIDFromContextGetterFn = func(context.Context) string {
		return middleware.GetRequestId(ctx)
	}
	logger.GetRequestIDFromContextFn = getRequestIDFromContextFn

	srv, err := b.initService(ctx, cfg)
	if err != nil {
		return unaryInterceptor, publicMiddleware, err
	}
	b.customApp.publicAPI = publicapi.NewPublicAPI(srv)
	b.customApp.adminApi = adminapi.NewAdminAPI(srv)
	b.customApp.externalApi = externalapi.NewExternalApi(srv)
	return unaryInterceptor, publicMiddleware, nil
}

func NewAuthServerInterceptor(cfg config.Config) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		logger.Info(ctx, "Method: %s", info.FullMethod)

		if strings.HasPrefix(info.FullMethod, "/tickets.external_api.ExternalApi/") {

			token, err := pkgmw.GetAccessTokenFromHeaders(ctx)
			if err != nil {
				logger.Error(ctx, "AUTH GetAccessTokenFromHeaders error %s", err)
				return nil, err
			}

			if token != cfg.ExternalApiToken {

				logger.Error(ctx, "AUTH External wrong token %s", token)

				return nil, errors.AccessDenied.Err("Неверный токен").
					WithLogKV("expect", cfg.ExternalApiToken, "actual", token)
			}
			return handler(ctx, req)
		} else if !strings.HasPrefix(info.FullMethod, "/tickets.public.PublicAPI/Health") {

			jwt := pkgmw.GetAuthJwt(ctx, cfg.Secrets.RsaPublicKey)
			if jwt == nil {
				return nil, errors.AccessDenied.Err("error with getting token")
			}

			md, _ := metadata.FromIncomingContext(ctx)
			intPPId := md.Get(internalPpHeader)
			extPPId := md.Get(externalPpHeader)
			ctxWithPpIds := ctx
			if len(intPPId) > 0 {
				ctxWithPpIds = context.WithValue(ctx, internalKey, intPPId[0])
			}
			if len(extPPId) > 0 {
				ctxWithPpIds = context.WithValue(ctxWithPpIds, externalKey, extPPId[0])
			}

			allowed := []string{
				"/tickets.public.PublicAPI/CreateTicket",
				"/tickets.public.PublicAPI/GetHistoryGoodsAndBox",
				"/tickets.public.PublicAPI/CreateTicketBoxOverdueShk",
				"/tickets.public.PublicAPI/GetUrlV2",
				"/tickets.public.PublicAPI/CreateSupportLogAsUser",
				"/tickets.public.PublicAPI/SetUserTicketState",
				"/tickets.public.PublicAPI/GetTicketTypes",
				"/tickets.public.PublicAPI/GetTicketStates",
				"/tickets.public.PublicAPI/GetUsersTicketsV2",
				"/tickets.public.PublicAPI/GetTicketTypesForUsers",
				"/tickets.public.PublicAPI/GetTicketReasons",
				"/tickets.public.PublicAPI/GetOverdueMarksByShks",
			}
			if jwt.InBlacklist && !slices.Contains(allowed, info.FullMethod) {
				return nil, errors.AccessDenied.Err("Вы находитесь в черном списке, данная функциональность вам недоступна")
			}

			if strings.HasPrefix(info.FullMethod, "/tickets.admin.AdminAPI/") && !jwt.IsAdmin {
				return nil, errors.AccessDenied.Err("you need to provide admin token")
			}

			return handler(ctxWithPpIds, req)
		}

		return handler(ctx, req)
	}
}

func (b *builder) initService(ctx context.Context, cfg config.Config) (*service.Service, error) {
	var (
		db             *pgxpool.Pool
		externalMainDB *pgxpool.Pool
		err            error
	)

	var dbUrl string
	if cfg.Env == "local" {
		dbUrl = cfg.LocalDbUrl
	} else {
		dbUrl = cfg.DbURL
	}

	// ОСНОВНАЯ БД
	db, err = pgxpool.Connect(ctx, dbUrl)
	if err != nil {
		return &service.Service{}, err
	}
	b.AddToPublicCloser("tickets.db.main", db.Close)

	// ВНЕШНЯЯ БД МЕЙН
	externalMainDB, err = pgxpool.Connect(ctx, cfg.MainDbUrlSlave)
	if err != nil {
		return &service.Service{}, err
	}
	b.AddToPublicCloser("external.db.main", externalMainDB.Close)

	// ОСНОВНАЯ БАЗА ДЛЯ ЧТЕНИПЯ
	dbSlave, err := pgxpool.Connect(ctx, cfg.DbURLSlave)
	if err != nil {
		return &service.Service{}, err
	}
	b.AddToPublicCloser("tickets.db.slave", dbSlave.Close)

	pgxRepo := pgxRepository.NewRepository(db, dbSlave, externalMainDB)

	// GRPC АПИШКИ
	// WRITE-OFF
	writeOffClientConnection, err := grpc.Dial(cfg.WriteOffApi, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error connection to service write-off: %w", err)
	}
	writeOffClient := write_off.NewExternalAPIClient(writeOffClientConnection)
	b.AddToPublicCloserWithError("write-off.grpc", writeOffClientConnection.Close)

	externalApiClientConnection, err := grpc.Dial(cfg.ExternalApi, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error connection to service external-api: %w", err)
	}
	externalApiClient := external.NewExternalAPIClient(externalApiClientConnection)
	b.AddToPublicCloserWithError("external-api.grpc", externalApiClientConnection.Close)

	backOfficeApiClientConnection, err := grpc.Dial(cfg.BackOfficeApi, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error connection to service backoffice-api: %w", err)
	}
	backOfficeApiClient := backoffice.NewBackofficeAPIClient(backOfficeApiClientConnection)
	b.AddToPublicCloserWithError("backoffice.grpc", backOfficeApiClientConnection.Close)

	storageClient := storage.NewClient(
		cfg.StorageUrl,
		cfg.StorageNewEl, // пока что работает только el кластер
		cfg.StorageNewEl,
		cfg.StorageNewEl,
		cfg.StorageImageHost,
		cfg.StoragePublicBucket,
		cfg.StoragePrivateBucket,
		cfg.StorageAuth,
		cfg.StorageNewAuth1,
		cfg.StorageStaticAuth,
		cfg.StorageStaticBucket,
		cfg.StorageImageBucket,
		cfg.StorageVideoBucket,
		0,
		cfg.StorageNewImageBasket,
		cfg.StorageStaticImageBasket,
		cfg.StorageCameraFragmentsBasket,
		cfg.StorageImageDocStaticHost,
		cfg.StorageImageDocNewFolderHost,
		cfg.StorageVideoStaticHost,
		cfg.StorageCameraFragmentStaticHost,
	)

	chunksService, _, err := chunks.New(cfg.RedisChunkUrl)
	if err != nil {
		return nil, fmt.Errorf("error connection to storage: %w", err)
	}

	boxApiConnection, err := grpc.Dial(cfg.ReturnedBoxApi, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error connection to service box-api: %w", err)
	}
	returnedBoxApiClient := returned_boxes.NewGrpcAPIClient(boxApiConnection)
	b.AddToPublicCloserWithError("returned-boxes.grpc", boxApiConnection.Close)

	redisCacheClient := goRedis.NewClient(
		&goRedis.Options{
			Addr:     cfg.RedisCacheAddress,
			Password: cfg.RedisCachePassword,
			DB:       0, // use default DB
		})

	ping := redisCacheClient.Ping()
	if ping.Err() != nil {
		return nil, ping.Err()
	}

	b.AddToPublicCloserWithError("redis.cache.tickets", redisCacheClient.Close)

	userInfoHeaders := map[string]string{"X-Auth-Id": cfg.UserInfoAuthId, "X-Auth-Sign": cfg.UserInfoAuthSign}
	userInfo := userinfo.New(cfg.UserInfoApi, userInfoHeaders, func(ms int64) {
		prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "exd_user_info_process_time",
			Help:    "Time of UserInfo service response",
			Buckets: []float64{1, 10, 100, 1000, 10000},
		})
	})

	apiShard := api_shard2.NewClient(cfg.ExternalApiToken, api_shard2.ApiShardURI)
	backOfficeApiHttpClient := backoffice_api.NewBackOfficeApi(cfg.WbPointApi)

	bertApiClient := api_shard.NewBertClient(api_shard.BertUrl)

	cacheClient := cache.NewCache(redisCacheClient)

	hrEmployees := hr_employees.New(cfg.HrEmployeesApiHost, cfg.GetHrEmployeesHeaders())

	cardsClient := cards.New(nil, cfg.GoodsRepoApiUrl, ServiceName, func(ms int64) {
	})

	srvRaitingClient := raiting.New(
		http_client.New(strings.Split(cfg.RaitingUrl, ";")), cfg.RaitingToken)

	srvBalanceClient := balance.New(
		http_client.New(strings.Split(cfg.BalanceUrl, ";")), cfg.BalanceToken)

	var authDiscoveryKafka sasl.Mechanism

	if cfg.Env == stageEnv {
		authDiscoveryKafka, err = scram.Mechanism(
			scram.SHA256,
			cfg.KafkaDiscoveryUserName,
			cfg.KafkaDiscoveryPassword,
		)
		if err != nil {
			return nil, fmt.Errorf("error creating mechanism for discovery kafka: %v", err)
		}
	} else {
		authDiscoveryKafka = plain.Mechanism{
			Username: cfg.KafkaDiscoveryUserName,
			Password: cfg.KafkaDiscoveryPassword,
		}
	}

	discoveryKafka := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaDiscoveryServers...),
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
		Async:        false,
		Transport: &kafka.Transport{
			SASL: authDiscoveryKafka,
		},
	}

	// main kafka
	mainKafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaMain.Servers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Millisecond,
		Transport: &kafka.Transport{
			SASL: plain.Mechanism{
				Username: cfg.KafkaMain.Username,
				Password: cfg.KafkaMain.Password,
			},
		},
	}

	var ticketsKafkaMechanism sasl.Mechanism

	switch cfg.KafkaTickets.SaslMechanism {
	case scram.SHA256.Name(), "sha256":
		ticketsKafkaMechanism, err = scram.Mechanism(scram.SHA256, cfg.KafkaTickets.Username, cfg.KafkaTickets.Password)
		if err != nil {
			logger.Fatal(ctx, "cannot create sha256Mechanism")
		}
	default:
		ticketsKafkaMechanism = plain.Mechanism{
			Username: cfg.KafkaTickets.Username,
			Password: cfg.KafkaTickets.Password,
		}
	}

	// tickets kafka
	ticketsKafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaTickets.Servers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Millisecond,
		Transport: &kafka.Transport{
			SASL: ticketsKafkaMechanism,
		},
	}

	srv := service.NewService(pgxRepo, cfg,
		writeOffClient,
		externalApiClient,
		backOfficeApiClient,
		storageClient,
		chunksService,
		cacheClient,
		userInfo,
		apiShard,
		backOfficeApiHttpClient,
		returnedBoxApiClient,
		bertApiClient,
		hrEmployees,
		cardsClient,
		srvRaitingClient,
		discoveryKafka,
		mainKafkaWriter,
		ticketsKafkaWriter,
		srvBalanceClient,
	)

	// читаю из кафки
	kafkaMainTicketsSubsidy, kafkaMainTicketsSubsidyCloser, err := kafkasub.New(model.SubsidyKafka,
		cfg.KafkaMain.ClientId,
		cfg.KafkaMain.Topic,
		cfg.KafkaMain.ConsumerGroup,
		cfg.KafkaMain.Username,
		cfg.KafkaMain.Password,
		cfg.KafkaMain.SaslMechanism,
		cfg.KafkaMain.Servers,
		cfg.KafkaMain.MinBytes,
		cfg.KafkaMain.MaxBytes,
		1,
		cfg.KafkaMain.CommitInterval,
		time.Second*10,
		func() {},
		1,
	)

	if err != nil {
		return nil, err
	}
	b.publicCloser.Add(model.SubsidyKafka, kafkaMainTicketsSubsidyCloser)
	b.customApp.TicketsKafkaProcessor = append(b.customApp.TicketsKafkaProcessor, NewKafkaLimitProcessor(kafkaMainTicketsSubsidy, func(msgs []kafka.Message) error {
		return srv.ProcessKafkaTicketsSubsidyMsg(ctx, msgs)
	}))

	// джобы
	err = b.initCrons(srv, cfg)
	if err != nil {
		return nil, err
	}

	//греем кеш
	err = srv.WarmingUpCache(ctx)
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func (b *builder) initCrons(srv *service.Service, cfg config.Config) error {
	// cron jobs
	// Starting redis pool
	rdb := &garyRedisPool.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (garyRedisPool.Conn, error) { return garyRedisPool.DialURL(cfg.RedisCronUrl) },
	}

	// Verify redis connection is working
	conn := rdb.Get()
	_, err := conn.Do("ping")
	if err != nil {
		return fmt.Errorf("error connecting to redis: %w", err)
	}
	err = conn.Close()
	if err != nil {
		return fmt.Errorf("error Close ping conn to redis: %w", err)
	}

	err = cron.InitCron(rdb, srv)
	if err != nil {
		logger.Fatal(logger.App, "init cron failed: %v", err)
	}

	b.AddToPublicCloserWithError("tickets.cron", rdb.Close)

	return nil
}

func connectAndDoMigration(ctx context.Context, sqlDns string, env string, migrationOff bool) error {
	sqlDns = sqlDns[:strings.LastIndex(sqlDns, "sslmode=disable")+15]
	connector, err := pq.NewConnector(sqlDns)
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)

	if !migrationOff {
		migrations := &migrate.FileMigrationSource{
			Dir: "./db/migrations/",
		}

		n, err := migrate.Exec(db, "postgres", migrations, migrate.Up)
		if err != nil {
			logger.Error(ctx, "Error migration: %v", err)
			return err
		}

		fmt.Printf("Applied %d migrations on env: %s!\n", n, env)
	}
	return nil
}

func (b *builder) AddToPublicCloser(name string, closeFn func()) {
	b.publicCloser.Add(name, func() error {
		app.GracefulDelay(name)
		closeFn()
		logger.Info(logger.App, fmt.Sprintf("%s: gracefully stopped", name))
		return nil
	})
}

func (b *builder) AddToPublicCloserWithError(name string, closeFn func() error) {
	b.publicCloser.Add(name, func() error {
		app.GracefulDelay(name)
		err := closeFn()
		if err != nil {
			return err
		}
		logger.Info(logger.App, fmt.Sprintf("%s: gracefully stopped", name))
		return nil
	})
}
