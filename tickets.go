package pgxRepository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	logger "gitlab.wildberries.ru/ext-delivery/ext-delivery/gopkg-logger"

	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
	"gitlab.wildberries.ru/ext-delivery/extdelivery-revision/tickets/internal/app/model"
)

func (r *PGXRepository) CreateTicket(ctx context.Context, userId, ticketTypeId int64, description, appVersion string, pickPointId int64, imageUrls, videoUrls, documentUrls []string, ticketReasonId int64, appSource model.ClientType, disableAutoclose bool, pickpointOrderId int64) (int64, error) {
	sqlQuery := `
insert into tickets(
user_id, description, ticket_type, ticket_state, pickpoint_id, image_urls, video_urls,
document_urls, ticket_reason_id, app_source, app_version, disable_autoclose, pickpoint_order_id)
				values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING id`

	var ticketId int64
	err := r.db.QueryRow(ctx, sqlQuery, userId, description, ticketTypeId, model.OpenTicketState,
		pickPointId, pq.Array(imageUrls), pq.Array(videoUrls), pq.Array(documentUrls),
		ticketReasonId, appSource, appVersion, disableAutoclose, pickpointOrderId).Scan(&ticketId)
	if err != nil {
		return 0, err
	}

	return ticketId, nil
}

func (r *PGXRepository) DeleteTicket(ctx context.Context, ticketId int64) (int64, error) {
	sqlQuery := `delete from tickets where id = $1`

	_, err := r.db.Exec(ctx, sqlQuery, ticketId)
	if err != nil {
		return 0, err
	}

	return ticketId, nil
}

func (r *PGXRepository) IsTicketWithTypeAlreadyExist(ctx context.Context, userId, ticketTypeId, pickPointId int64) (int64, error) {

	query := `select id
			  from tickets r
			  where
			  	r.user_id = $1
			  	and r.ticket_type = $2
			  	and r.pickpoint_id = $3
			  limit 1`

	var existId int64
	err := r.dbSlave.QueryRow(ctx, query, userId, ticketTypeId, pickPointId).Scan(&existId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return existId, nil
}

func (r *PGXRepository) IsInProgressTicketWithTypeAlreadyExist(ctx context.Context, userId *int64, ticketTypeId, pickPointId int64) (int64, error) {

	if ticketTypeId == model.SuspendedShkTicketTypeId || ticketTypeId == model.SuspendedBrandedShkTicketTypeId || ticketTypeId == model.ExpensiveGoodsControlTicketTypeId {
		return 0, nil
	}

	query := `select id
			  from tickets r
			  where
			  	(cast($1 as bigint) is null or r.user_id = $1)
			  	and r.ticket_state != 3
			  	and r.ticket_type = $2
			  	and ((($2 in (17,10)) and r.pickpoint_id in (select i.pickpoint_id from managers_pickpoints_info i where i.manager_id = (select id from managers where user_id = $1)))
			  		or r.pickpoint_id  = $3)
			  	and ((($2 in (17,10)) and r.create_date >= (now() at time zone ('utc')) - '1 month'::interval)
			  		or ((select owner_only from tickets_types where tickets_types.id = $2) is true and r.create_date >= (now() at time zone ('utc')) - '2 day'::interval)
			  		or r.create_date >= (now() at time zone ('utc')) - '1 day'::interval)
			  limit 1`

	var existId int64
	err := r.dbSlave.QueryRow(ctx, query, userId, ticketTypeId, pickPointId).Scan(&existId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return existId, nil
}

func (r *PGXRepository) SetTicketState(ctx context.Context, managerId, ticketId int64, ticketStateId model.TicketStateId) (int64, error) {

	var affected int64
	switch ticketStateId {
	case model.InProgressTicketState, model.WaitingPaymentTicketState, model.InactiveTicketState, model.OpenTicketState:
		if managerId != 0 {
			query := `update tickets set ticket_state=$1, manager_id=$2, update_date=(now() at time zone ('utc')) where id=$3 and manager_id = 0;`
			ex, err := r.db.Exec(ctx, query, ticketStateId, managerId, ticketId)
			if err != nil {
				return 0, err
			}
			affected = ex.RowsAffected()
		} else {
			query := `update tickets set ticket_state=$1 where id=$2;`
			_, err := r.db.Exec(ctx, query, ticketStateId, ticketId)
			if err != nil {
				return 0, err
			}
		}
	case model.ClosedTicketState:
		query := `update tickets set finish_date=(now() at time zone ('utc')), ticket_state = 3 where id=$1`
		ex, err := r.db.Exec(ctx, query, ticketId)
		if err != nil {
			return 0, err
		}
		affected = ex.RowsAffected()
	}

	return affected, nil
}

func (r *PGXRepository) GetCurrentTicketState(ctx context.Context, ticketId int64) (model.TicketStateId, error) {
	query := `select ticket_state from tickets
              where id = $1`

	var currentTicketStateId model.TicketStateId
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&currentTicketStateId)
	if err != nil {
		return 0, err
	}

	return currentTicketStateId, nil
}

func (r *PGXRepository) GetTicketType(ctx context.Context, ticketId int64) (int64, error) {
	query := `select ticket_type from tickets where id = $1`

	var ticketTypeId int64
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&ticketTypeId)
	if err != nil {
		return 0, err
	}
	return ticketTypeId, nil
}

func (r *PGXRepository) GetTicketState(ctx context.Context, ticketId int64) (int64, error) {
	query := `select ticket_state from tickets where id = $1`

	var ticketStateId int64
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&ticketStateId)
	if err != nil {
		return 0, err
	}
	return ticketStateId, nil
}

func (r *PGXRepository) GetTicketCountByTicketType(ctx context.Context, ticketTypeId int64, userId int64) (int64, error) {

	query := "select count(id) from tickets where ticket_type=$1 and ticket_state != 3 and user_id = $2"

	var count int64
	err := r.dbSlave.QueryRow(ctx, query, ticketTypeId, userId).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r *PGXRepository) GetAllTicketsByTicketStateId(ctx context.Context, ticketStateId model.TicketStateId) ([]*model.Ticket, error) {

	var (
		ticketStateIdSql *int64
	)

	if ticketStateId > 0 {
		tsi := int64(ticketStateId)
		ticketStateIdSql = &tsi
	}

	query := `select t.id, t.pickpoint_id, t.user_id, pp.address, pp.pickpoint_id, pp.phone, pp.user_name, 
       			m.grade, m.note , t.create_date, t.finish_date, t.description, t.ticket_type, tt.description as ticket_type_name, 
       			t.ticket_state, t.image_urls, t.open_after_answer, t.is_read, t.video_urls, t.document_urls, 
       			t.manager_id, t.update_date, t.disable_autoclose
		      from tickets t
		      left join tickets_types tt ON  tt.id=t.ticket_type
					left join managers_grades m on m.ticket_id=t.id
					left  join managers_pickpoints_info pp on  t.manager_id=pp.manager_id  and pp.pickpoint_id=t.pickpoint_id 
			  where (cast($1 as numeric) is null or t.ticket_state = $1)`

	rows, err := r.dbSlave.Query(
		ctx, query, ticketStateIdSql)

	if err != nil {
		if err == pgx.ErrNoRows {
			return []*model.Ticket{}, nil
		}
		return []*model.Ticket{}, err
	}

	result := make([]*model.Ticket, 0)

	defer rows.Close()
	for rows.Next() {
		t := model.Ticket{}
		t.PickPointInfo = &model.PickPointInfo{}
		var (
			ppInfoAddress     sql.NullString
			ppInfoPhone       sql.NullInt64
			ppInfoName        sql.NullString
			ppInfoPickPointId sql.NullInt64
			ticketGrade       sql.NullInt64
			gradeComment      sql.NullString
			finishDate        sql.NullString
			imageUrls         []sql.NullString
			videoUrls         []sql.NullString
			documentUrls      []sql.NullString
		)
		err = rows.Scan(&t.Id, &t.PickPointId, &t.UserId, &ppInfoAddress, &ppInfoPickPointId, &ppInfoPhone, &ppInfoName, &ticketGrade, &gradeComment,
			&t.CreateDate, &finishDate, &t.Description, &t.TicketTypeId, &t.TicketTypeName, &t.TicketStateId, pq.Array(&imageUrls), &t.OpenAfterAnswer, &t.IsRead,
			pq.Array(&videoUrls), pq.Array(&documentUrls), &t.ManagerId, &t.UpdateDate, &t.DisableAutoclose)
		if err != nil {
			return nil, err
		}
		if ppInfoAddress.Valid {
			t.PickPointInfo.Address = ppInfoAddress.String
		}
		if ppInfoPhone.Valid {
			t.PickPointInfo.Phone = ppInfoPhone.Int64
		}
		if ppInfoName.Valid {
			t.PickPointInfo.Name = ppInfoName.String
		}
		if ppInfoPickPointId.Valid {
			t.PickPointInfo.PickPointId = ppInfoPickPointId.Int64
		}
		if ticketGrade.Valid {
			t.TicketGrade = ticketGrade.Int64
		}
		if gradeComment.Valid {
			t.GradeComment = gradeComment.String
		}
		if finishDate.Valid {
			parsedDate, err := time.Parse(model.TicketDatesFromDbLayout, finishDate.String)
			if err == nil {
				t.FinishDate = &parsedDate
			}
		}
		for _, iu := range imageUrls {
			if iu.Valid {
				t.ImageUrls = append(t.ImageUrls, iu.String)
			}
		}
		for _, du := range documentUrls {
			if du.Valid {
				t.DocumentUrls = append(t.DocumentUrls, du.String)
			}
		}
		for _, vu := range videoUrls {
			if vu.Valid {
				t.VideoUrls = append(t.VideoUrls, vu.String)
			}
		}
		result = append(result, &t)
	}

	return result, rows.Err()
}

func (r *PGXRepository) GetAllTicketsByTicketStateAndTicketTypes(ctx context.Context, ticketStateId model.TicketStateId, ticketTypes []int64) ([]*model.Ticket, error) {

	var (
		ticketStateIdSql *int64
	)

	if ticketStateId > 0 {
		tsi := int64(ticketStateId)
		ticketStateIdSql = &tsi
	}

	query := `select t.id, t.pickpoint_id, t.user_id, pp.address, pp.pickpoint_id, pp.phone, pp.user_name, 
       			m.grade, m.note , t.create_date, t.finish_date, t.description, t.ticket_type, tt.description as ticket_type_name, 
       			t.ticket_state, t.image_urls, t.open_after_answer, t.is_read, t.video_urls, t.document_urls, 
       			t.manager_id, t.update_date, t.disable_autoclose
		      from tickets t
		      left join tickets_types tt ON  tt.id=t.ticket_type
					left join managers_grades m on m.ticket_id=t.id
					left  join managers_pickpoints_info pp on  t.manager_id=pp.manager_id  and pp.pickpoint_id=t.pickpoint_id 
			  where (cast($1 as numeric) is null or t.ticket_state = $1) and t.ticket_type = any($2)`

	rows, err := r.dbSlave.Query(ctx, query, ticketStateIdSql, pq.Array(ticketTypes))

	if err != nil {
		if err == pgx.ErrNoRows {
			return []*model.Ticket{}, nil
		}
		return []*model.Ticket{}, err
	}

	result := make([]*model.Ticket, 0)

	defer rows.Close()
	for rows.Next() {
		t := model.Ticket{}
		t.PickPointInfo = &model.PickPointInfo{}
		var (
			ppInfoAddress     sql.NullString
			ppInfoPhone       sql.NullInt64
			ppInfoName        sql.NullString
			ppInfoPickPointId sql.NullInt64
			ticketGrade       sql.NullInt64
			gradeComment      sql.NullString
			finishDate        sql.NullString
			imageUrls         []sql.NullString
			videoUrls         []sql.NullString
			documentUrls      []sql.NullString
		)
		err = rows.Scan(&t.Id, &t.PickPointId, &t.UserId, &ppInfoAddress, &ppInfoPickPointId, &ppInfoPhone, &ppInfoName, &ticketGrade, &gradeComment,
			&t.CreateDate, &finishDate, &t.Description, &t.TicketTypeId, &t.TicketTypeName, &t.TicketStateId, pq.Array(&imageUrls), &t.OpenAfterAnswer, &t.IsRead,
			pq.Array(&videoUrls), pq.Array(&documentUrls), &t.ManagerId, &t.UpdateDate, &t.DisableAutoclose)
		if err != nil {
			return nil, err
		}
		if ppInfoAddress.Valid {
			t.PickPointInfo.Address = ppInfoAddress.String
		}
		if ppInfoPhone.Valid {
			t.PickPointInfo.Phone = ppInfoPhone.Int64
		}
		if ppInfoName.Valid {
			t.PickPointInfo.Name = ppInfoName.String
		}
		if ppInfoPickPointId.Valid {
			t.PickPointInfo.PickPointId = ppInfoPickPointId.Int64
		}
		if ticketGrade.Valid {
			t.TicketGrade = ticketGrade.Int64
		}
		if gradeComment.Valid {
			t.GradeComment = gradeComment.String
		}
		if finishDate.Valid {
			parsedDate, err := time.Parse(model.TicketDatesFromDbLayout, finishDate.String)
			if err == nil {
				t.FinishDate = &parsedDate
			}
		}
		for _, iu := range imageUrls {
			if iu.Valid {
				t.ImageUrls = append(t.ImageUrls, iu.String)
			}
		}
		for _, du := range documentUrls {
			if du.Valid {
				t.DocumentUrls = append(t.DocumentUrls, du.String)
			}
		}
		for _, vu := range videoUrls {
			if vu.Valid {
				t.VideoUrls = append(t.VideoUrls, vu.String)
			}
		}
		result = append(result, &t)
	}

	return result, rows.Err()
}

func (r *PGXRepository) GetTicketById(ctx context.Context, ticketId int64) (*model.Ticket, error) {

	query := `select t.id, t.pickpoint_id, t.user_id, pp.address, pp.pickpoint_id, pp.phone, pp.user_name, 
       			m.grade, m.note , t.create_date, t.finish_date, t.description, t.ticket_type, tt.description as ticket_type_name, 
       			t.ticket_state, t.image_urls, t.open_after_answer, t.is_read, t.video_urls, t.document_urls, 
       			t.manager_id, t.update_date, t.pickpoint_order_id, po.inn
		      from tickets t
		      left join tickets_types tt ON  tt.id=t.ticket_type
					left join managers_grades m on m.ticket_id=t.id
					left join managers_pickpoints_info pp on
						((t.manager_id=pp.manager_id or (t.manager_id = 0 and pp.manager_id = (select id from managers where user_id = t.user_id)))
						and pp.pickpoint_id=t.pickpoint_id)
		      		left join managers_pickpoints_info pp_owner on t.pickpoint_id = pp_owner.pickpoint_id and pp_owner.is_owner = true
		      		left join pickpoint_owner po on po.user_id = (select user_id from managers where id = pp_owner.manager_id)
			  where t.id = $1`

	rows, err := r.dbSlave.Query(ctx, query, ticketId)

	if err != nil {
		if err == pgx.ErrNoRows {
			return &model.Ticket{}, nil
		}
		return &model.Ticket{}, err
	}

	defer rows.Close()
	t := model.Ticket{}
	t.PickPointInfo = &model.PickPointInfo{}
	for rows.Next() {
		var (
			ppInfoAddress     sql.NullString
			ppInfoPhone       sql.NullInt64
			ppInfoName        sql.NullString
			ppInfoPickPointId sql.NullInt64
			ticketGrade       sql.NullInt64
			gradeComment      sql.NullString
			finishDate        sql.NullString
			imageUrls         []sql.NullString
			videoUrls         []sql.NullString
			documentUrls      []sql.NullString
			inn               sql.NullString
		)
		err = rows.Scan(&t.Id, &t.PickPointId, &t.UserId, &ppInfoAddress, &ppInfoPickPointId, &ppInfoPhone, &ppInfoName, &ticketGrade, &gradeComment,
			&t.CreateDate, &finishDate, &t.Description, &t.TicketTypeId, &t.TicketTypeName, &t.TicketStateId, pq.Array(&imageUrls), &t.OpenAfterAnswer, &t.IsRead,
			pq.Array(&videoUrls), pq.Array(&documentUrls), &t.ManagerId, &t.UpdateDate, &t.PickPointOrderId, &inn)
		if err != nil {
			return nil, err
		}
		if ppInfoAddress.Valid {
			t.PickPointInfo.Address = ppInfoAddress.String
		}
		if ppInfoPhone.Valid {
			t.PickPointInfo.Phone = ppInfoPhone.Int64
		}
		if ppInfoName.Valid {
			t.PickPointInfo.Name = ppInfoName.String
		}
		if ppInfoPickPointId.Valid {
			t.PickPointInfo.PickPointId = ppInfoPickPointId.Int64
		}
		if ticketGrade.Valid {
			t.TicketGrade = ticketGrade.Int64
		}
		if gradeComment.Valid {
			t.GradeComment = gradeComment.String
		}
		if finishDate.Valid {
			parsedDate, err := time.Parse(model.TicketDatesFromDbLayout, finishDate.String)
			if err == nil {
				t.FinishDate = &parsedDate
			}
		}
		if inn.Valid {
			t.Inn = inn.String
		}
		for _, iu := range imageUrls {
			if iu.Valid {
				t.ImageUrls = append(t.ImageUrls, iu.String)
			}
		}
		for _, du := range documentUrls {
			if du.Valid {
				t.DocumentUrls = append(t.DocumentUrls, du.String)
			}
		}
		for _, vu := range videoUrls {
			if vu.Valid {
				t.VideoUrls = append(t.VideoUrls, vu.String)
			}
		}
	}

	return &t, rows.Err()
}

func (r *PGXRepository) GetAllTicketsAdmin(ctx context.Context, filter *model.TicketFilter) (tickets []*model.Ticket, countOfTickets int64, err error) {
	var (
		orderByQuery string
		isManager    bool
		opts         []string
	)
	query := `SELECT t.id, t.ticket_type, t.ticket_state, t.create_date, t.pickpoint_id, coalesce(mg.grade,0),
       			t.finish_date, t.description, t.ticket_reason_id, coalesce(mg.note, ''), coalesce(t.app_source, 0),
       			t.image_urls, t.video_urls, t.document_urls, coalesce(tt.description,''), coalesce(tr.description, ''),
       			t.user_id, coalesce((select m.user_id from managers m where m.id=t.manager_id),-1)
					FROM tickets t 
						left join managers_grades mg on mg.ticket_id=t.id and mg.manager_id = t.manager_id
						left join tickets_types tt on t.ticket_type = tt.id
						left join tickets_reasons tr on t.ticket_reason_id = tr.id`

	queryCount := `SELECT count(t.id) FROM tickets t`

	if filter != nil {
		if len(filter.TicketIds) != 0 {
			var ticketsIds []string
			for _, t := range filter.TicketIds {
				ticketsIds = append(ticketsIds, fmt.Sprint(t))
			}
			opts = append(opts, fmt.Sprintf("t.id in (%s)", strings.Join(ticketsIds, ", ")))
		}

		if filter.PickpointId != 0 {
			opts = append(opts, fmt.Sprintf("t.pickpoint_id = %v", filter.PickpointId))
		}

		if len(filter.TicketTypeIds) > 0 {
			var ticketTypes []string
			for _, tt := range filter.TicketTypeIds {
				ticketTypes = append(ticketTypes, fmt.Sprint(tt))
			}
			opts = append(opts, fmt.Sprintf("t.ticket_type in (%s)", strings.Join(ticketTypes, ", ")))
		}

		if filter.TicketStateId != 0 {
			opts = append(opts, fmt.Sprintf("t.ticket_state = %v", filter.TicketStateId))
		}

		if filter.TicketAppSourceId != 0 {
			opts = append(opts, fmt.Sprintf("t.app_source = %v", filter.TicketAppSourceId))
		}

		if filter.IsOnlyMyTickets && len(filter.UserIds) > 0 {
			var userIds []string
			for _, uid := range filter.UserIds {
				userIds = append(userIds, fmt.Sprint(uid))
			}
			opts = append(opts, fmt.Sprintf("t.user_id in (%s)", strings.Join(userIds, ", ")))
		}

		if filter.ManagerId != 0 {
			opts = append(opts, fmt.Sprintf("t.manager_id = %v", filter.ManagerId))
		}

		if filter.DateFrom != "" && filter.DateTo != "" {
			var opt string
			if model.TicketStateId(filter.TicketStateId) == model.ClosedTicketState {
				opt = fmt.Sprintf("date(t.finish_date + '3hours'::interval) >= date('%s') and date(t.finish_date + '3hours'::interval) <= date('%s')", filter.DateFrom, filter.DateTo)
			} else {
				opt = fmt.Sprintf("date(t.create_date + '3hours'::interval) >= date('%s') and date(t.create_date + '3hours'::interval) <= date('%s')", filter.DateFrom, filter.DateTo)
			}
			opts = append(opts, opt)
		}

		//Если юзер не менеджер, тогда сортировать по дате создания тикета по убыванию (=id тикета desc)
		if len(filter.UserIds) == 1 {
			isManager, err = r.IsManager(ctx, filter.UserIds[0])
			if err != nil {
				logger.Error(ctx, "cannot check if manager role exists, err: %v", err)
			}
		}
		if !isManager {
			orderByQuery = fmt.Sprintf("order by t.id desc, t.ticket_state limit %v offset %v", filter.Limit, filter.Offset)
		} else {
			orderByQuery = fmt.Sprintf("order by t.id, t.ticket_state limit %v offset %v", filter.Limit, filter.Offset)
		}

		if len(opts) != 0 {
			where := fmt.Sprintf(" where %s", strings.Join(opts, " and "))
			query += where
			queryCount += where
		}
		query += fmt.Sprintf(" %s", orderByQuery)
	}

	err = r.dbSlave.QueryRow(ctx, queryCount).Scan(&countOfTickets)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.dbSlave.Query(ctx, query)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			logger.Error(ctx, err.Error())
			return nil, 0, nil
		}
		return nil, 0, err
	}

	defer rows.Close()

	for rows.Next() {
		var t model.Ticket
		err = rows.Scan(&t.Id, &t.TicketTypeId, &t.TicketStateId, &t.CreateDate, &t.PickPointId, &t.TicketGrade,
			&t.FinishDate, &t.Description, &t.TicketReasonId, &t.GradeComment, &t.AppSource, &t.ImageUrls, &t.VideoUrls,
			&t.DocumentUrls, &t.TicketTypeName, &t.TicketReasonName, &t.UserId, &t.ManagersUserId)

		if err != nil {
			return nil, 0, err
		}
		tickets = append(tickets, &t)
	}

	return tickets, countOfTickets, rows.Err()
}

func (r *PGXRepository) GetAllTickets(ctx context.Context, ticketIds []int64,
	pickpointId, ticketTypeId, ticketReasonId, ticketStateId, appSourceId int64,
	dateFrom, dateTo *time.Time, isRead, isOnlyMyTickets bool, userIds []int64, managerId, limit, offset int64,
	search string, sortByLatestAnswer bool, sortByCreateDate string, onlyAnsweredByUser bool) ([]*model.Ticket, error) {

	query :=
		`select 
	t.id, t.pickpoint_id, t.user_id, 
	pp.address, pp.phone, pp.user_name,
	mg.grade, mg.note,
	t.create_date, t.finish_date, t.description, t.ticket_type,
	tt.description as ticket_type_name, t.ticket_state, t.image_urls,
	t.open_after_answer, t.is_read, t.video_urls, t.document_urls,
	t.manager_id, t.update_date, 
	po.inn, t.ticket_reason_id, coalesce(t.app_source,0)
from tickets t
	left join tickets_types tt ON tt.id=t.ticket_type
	left join managers_grades mg ON mg.ticket_id=t.id and mg.manager_id = t.manager_id
	left join managers m_ticket_creator ON m_ticket_creator.user_id = t.user_id
	left join managers_pickpoints_info pp ON pp.manager_id = m_ticket_creator.id and pp.pickpoint_id=t.pickpoint_id
	left join managers_pickpoints_info pp_owner ON t.pickpoint_id = pp_owner.pickpoint_id and pp_owner.is_owner = true
	left join managers pp_owner_user_id ON pp_owner_user_id.id = pp_owner.manager_id
	left join pickpoint_owner po ON po.user_id = pp_owner_user_id.user_id
	LEFT JOIN LATERAL (SELECT max(create_date) create_date FROM support_log WHERE ticket_id=t.id) lcd ON true
	LEFT JOIN LATERAL (SELECT CASE WHEN _m.role = 'admin' THEN 1 ELSE 0 END role_order 
						FROM support_log _sl, managers _m 
						WHERE _sl.ticket_id=t.id AND _sl.create_date=lcd.create_date AND _m.user_id=_sl.user_id) lro ON true`

	whereClause := "\nWHERE"

	addedClauseToWhere := false
	if len(ticketIds) > 0 {
		addedClauseToWhere = true
		anyTicketIds := ""
		for i, ticket := range ticketIds {
			anyTicketIds += strconv.FormatInt(ticket, 10)
			if i != len(ticketIds)-1 {
				anyTicketIds += ", "
			}
		}
		whereClause += fmt.Sprintf(" t.id = any('{%s}')", anyTicketIds)
	}

	if ticketTypeId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.ticket_type = %d", ticketTypeId)
	}

	if ticketStateId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.ticket_state = %d", ticketStateId)
	}

	if ticketReasonId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.ticket_reason_id = %d", ticketReasonId)
	}

	if dateFrom != nil {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		dateFrom.Add(-3 * time.Hour)
		whereClause += fmt.Sprintf(
			" (t.ticket_state != 3 or t.finish_date >= '%s')", dateFrom.Format("2006-01-02"))
		whereClause += fmt.Sprintf(
			" AND (t.ticket_state not in (1,2) or t.create_date >= '%s')", dateFrom.Format("2006-01-02"))
	}

	if dateTo != nil {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		dateFrom.Add(3 * time.Hour)
		whereClause += fmt.Sprintf(
			" (t.ticket_state != 3 or t.finish_date <= '%s')", dateTo.Format("2006-01-02"))
		whereClause += fmt.Sprintf(
			" AND (t.ticket_state not in (1,2) or t.create_date <= '%s')", dateTo.Format("2006-01-02"))
	}

	if isRead {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.is_read = true")
	}

	if isOnlyMyTickets && len(userIds) > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		anyUserIDs := ""
		for i, user := range userIds {
			anyUserIDs += strconv.FormatInt(user, 10)
			if i != len(userIds)-1 {
				anyUserIDs += ", "
			}
		}
		whereClause += fmt.Sprintf(" t.id = any('{%s}')", anyUserIDs)
	}

	if managerId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.manager_id = %d", managerId)
	}

	if len(search) > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(
			" (t.description like '%%%s%%' or tt.description like '%%%s%%' or cast(t.id as text) like '%%%s%%')",
			search, search, search)
	}

	if pickpointId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.pickpoint_id = %d", pickpointId)
	}

	if appSourceId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.app_source = %d", appSourceId)
	}

	if onlyAnsweredByUser {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += ` lro.role_order = 0`
	}

	var isManager bool
	var err error
	if len(userIds) == 1 {
		isManager, err = r.IsManager(ctx, userIds[0])
		if err != nil {
			logger.Error(ctx, "cannot check if manager role exists, err: %v", err)
		}
	}

	if !isManager && pickpointId == 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf("%s %v", ` t.user_id = `, userIds[0])
	}

	if addedClauseToWhere {
		query += whereClause
	}

	orderByQuery := ` order by`

	if sortByLatestAnswer {
		orderByQuery += ` lro.role_order, lcd.create_date desc,`
	}
	switch sortByCreateDate {
	case "asc":
		orderByQuery += ` t.create_date asc,`
	case "desc":
		orderByQuery += ` t.create_date desc,`
	}

	//Если юзер не менеджер, тогда сортировать по дате создания тикета по убыванию (=id тикета desc)
	if !isManager {
		orderByQuery += ` (t.ticket_state=3 or t.ticket_state=4), t.open_after_answer, (t.app_source=2) desc nulls last, (ticket_type=40 or ticket_type=42) desc`
	} else {
		orderByQuery += ` t.id, t.ticket_state`
	}
	orderByQuery += fmt.Sprintf(" limit %d offset %d", limit, offset)

	query += orderByQuery
	rows, err := r.dbSlave.Query(ctx, query)

	if err != nil {
		if err == pgx.ErrNoRows {
			return []*model.Ticket{}, nil
		}
		return []*model.Ticket{}, fmt.Errorf("[GetAllTickets] failed to get all tickets: %w", err)
	}

	result := make([]*model.Ticket, 0)

	defer rows.Close()
	for rows.Next() {
		t := model.Ticket{}
		t.PickPointInfo = &model.PickPointInfo{}
		var (
			ppInfoAddress sql.NullString
			ppInfoPhone   sql.NullInt64
			ppInfoName    sql.NullString
			ticketGrade   sql.NullInt64
			gradeComment  sql.NullString
			imageUrls     []sql.NullString
			videoUrls     []sql.NullString
			documentUrls  []sql.NullString
			inn           sql.NullString
		)
		err = rows.Scan(&t.Id, &t.PickPointId, &t.UserId, &ppInfoAddress, &ppInfoPhone, &ppInfoName, &ticketGrade,
			&gradeComment, &t.CreateDate, &t.FinishDate, &t.Description, &t.TicketTypeId, &t.TicketTypeName,
			&t.TicketStateId, pq.Array(&imageUrls), &t.OpenAfterAnswer, &t.IsRead, pq.Array(&videoUrls),
			pq.Array(&documentUrls), &t.ManagerId, &t.UpdateDate, &inn, &t.TicketReasonId, &t.AppSource)
		if err != nil {
			return nil, err
		}

		if ppInfoAddress.Valid {
			t.PickPointInfo.Address = ppInfoAddress.String
		}
		if ppInfoPhone.Valid {
			t.PickPointInfo.Phone = ppInfoPhone.Int64
		}
		if ppInfoName.Valid {
			t.PickPointInfo.Name = ppInfoName.String
		}
		t.PickPointInfo.PickPointId = t.PickPointId
		if ticketGrade.Valid {
			t.TicketGrade = ticketGrade.Int64
		}
		if gradeComment.Valid {
			t.GradeComment = gradeComment.String
		}
		if inn.Valid {
			t.Inn = inn.String
		}
		for _, iu := range imageUrls {
			if iu.Valid {
				t.ImageUrls = append(t.ImageUrls, iu.String)
			}
		}
		for _, du := range documentUrls {
			if du.Valid {
				t.DocumentUrls = append(t.DocumentUrls, du.String)
			}
		}
		for _, vu := range videoUrls {
			if vu.Valid {
				t.VideoUrls = append(t.VideoUrls, vu.String)
			}
		}
		result = append(result, &t)
	}

	return result, nil
}

func (r *PGXRepository) GetAllTicketsCount(ctx context.Context,
	pickpointId, ticketTypeId, ticketReasonId, ticketStateId, appSourceId int64, dateFrom, dateTo *time.Time,
	isRead, isOnlyMyTickets bool, userIds []int64, managerId int64, search string) (int64, error) {

	query :=
		`select count(t.id)
from tickets t
	left join tickets_types tt ON tt.id=t.ticket_type
	left join managers_grades mg ON mg.ticket_id=t.id and mg.manager_id = t.manager_id
	left join managers m_ticket_creator ON m_ticket_creator.user_id = t.user_id
	left join managers_pickpoints_info pp ON pp.manager_id = m_ticket_creator.id and pp.pickpoint_id=t.pickpoint_id
	left join managers_pickpoints_info pp_owner ON t.pickpoint_id = pp_owner.pickpoint_id and pp_owner.is_owner = true
	left join managers pp_owner_user_id ON pp_owner_user_id.id = pp_owner.manager_id
	left join pickpoint_owner po ON po.user_id = pp_owner_user_id.user_id`

	whereClause := "\nWHERE"

	addedClauseToWhere := false

	if ticketTypeId > 0 {
		addedClauseToWhere = true
		whereClause += fmt.Sprintf(" t.ticket_type = %d", ticketTypeId)
	}

	if ticketStateId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.ticket_state = %d", ticketStateId)
	}

	if ticketReasonId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.ticket_reason_id = %d", ticketReasonId)
	}

	if dateFrom != nil {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		dateFrom.Add(-3 * time.Hour)
		whereClause += fmt.Sprintf(
			" (t.ticket_state != 3 or t.finish_date >= '%s')", dateFrom.Format("2006-01-02"))
		whereClause += fmt.Sprintf(
			" AND (t.ticket_state not in (1,2) or t.create_date >= '%s')", dateFrom.Format("2006-01-02"))
	}

	if dateTo != nil {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		dateTo.Add(3 * time.Hour)
		whereClause += fmt.Sprintf(
			" (t.ticket_state != 3 or t.finish_date <= '%s')", dateTo.Format("2006-01-02"))
		whereClause += fmt.Sprintf(
			" AND (t.ticket_state not in (1,2) or t.create_date <= '%s')", dateTo.Format("2006-01-02"))
	}

	if isRead {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.is_read = true")
	}

	if isOnlyMyTickets && len(userIds) > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		anyUserIDs := ""
		for i, user := range userIds {
			anyUserIDs += strconv.FormatInt(user, 10)
			if i != len(userIds)-1 {
				anyUserIDs += ", "
			}
		}
		whereClause += fmt.Sprintf(" t.id = any(array[%s])", anyUserIDs)
	}

	if managerId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.manager_id = %d", managerId)
	}

	if len(search) > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(
			" (t.description like '%%%s%%' or tt.description like '%%%s%%' or cast(t.id as text) like '%%%s%%')",
			search, search, search)
	}

	if pickpointId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.pickpoint_id = %d", pickpointId)
	}

	if appSourceId > 0 {
		if addedClauseToWhere {
			whereClause += " AND"
		} else {
			addedClauseToWhere = true
		}
		whereClause += fmt.Sprintf(" t.app_source = %d", appSourceId)
	}

	if addedClauseToWhere {
		query += whereClause
	}

	rows, err := r.dbSlave.Query(ctx, query)

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	defer rows.Close()

	var allCount int64
	for rows.Next() {
		err = rows.Scan(&allCount)
		if err != nil {
			return 0, err
		}
	}

	return allCount, nil
}

func (r *PGXRepository) GetInProgressTicketForManagerIfExist(ctx context.Context, managerId int64) (*model.Ticket, error) {

	query := `select t.id, t.pickpoint_id, t.user_id, pp.address, pp.pickpoint_id, pp.phone, pp.user_name,
			   mg.grade, mg.note , t.create_date, t.finish_date, t.description, t.ticket_type, tt.description as ticket_type_name,
			   t.ticket_state, t.image_urls, t.open_after_answer, t.is_read, t.video_urls, t.document_urls,
			   t.manager_id, t.update_date, po.inn
		from tickets t
				 left join tickets_types tt ON  tt.id=t.ticket_type
				 left join managers_grades mg on mg.ticket_id=t.id and mg.manager_id = t.manager_id
				 left join managers m_ticket_creator on m_ticket_creator.user_id = t.user_id
				 left join managers_pickpoints_info pp on
					pp.manager_id = m_ticket_creator.id
				and pp.pickpoint_id=t.pickpoint_id
				 left join managers_pickpoints_info pp_owner on t.pickpoint_id = pp_owner.pickpoint_id and pp_owner.is_owner = true
				 left join managers pp_owner_user_id on pp_owner_user_id.id = pp_owner.manager_id
				 left join pickpoint_owner po on po.user_id = pp_owner_user_id.user_id
		where
		  t.ticket_state = 2
		  and t.open_after_answer = 1
		  and t.manager_id = $1
		order by t.open_after_answer desc, t.update_date, t.manager_id desc, t.create_date, t.id
		limit 1`

	rows, err := r.dbSlave.Query(ctx, query, managerId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return &model.Ticket{}, nil
		}
		return &model.Ticket{}, err
	}
	t := model.Ticket{}
	t.PickPointInfo = &model.PickPointInfo{}
	var (
		ppInfoAddress     sql.NullString
		ppInfoPhone       sql.NullInt64
		ppInfoName        sql.NullString
		ppInfoPickPointId sql.NullInt64
		ticketGrade       sql.NullInt64
		gradeComment      sql.NullString
		finishDate        sql.NullString
		imageUrls         []sql.NullString
		videoUrls         []sql.NullString
		documentUrls      []sql.NullString
		inn               sql.NullString
	)

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&t.Id, &t.PickPointId, &t.UserId, &ppInfoAddress, &ppInfoPickPointId, &ppInfoPhone, &ppInfoName, &ticketGrade, &gradeComment,
			&t.CreateDate, &finishDate, &t.Description, &t.TicketTypeId, &t.TicketTypeName, &t.TicketStateId, pq.Array(&imageUrls), &t.OpenAfterAnswer, &t.IsRead,
			pq.Array(&videoUrls), pq.Array(&documentUrls), &t.ManagerId, &t.UpdateDate, &inn)
		if err != nil {
			return &model.Ticket{}, err
		}
		if ppInfoAddress.Valid {
			t.PickPointInfo.Address = ppInfoAddress.String
		}
		if ppInfoPhone.Valid {
			t.PickPointInfo.Phone = ppInfoPhone.Int64
		}
		if ppInfoName.Valid {
			t.PickPointInfo.Name = ppInfoName.String
		}
		t.PickPointInfo.PickPointId = t.PickPointId
		if ticketGrade.Valid {
			t.TicketGrade = ticketGrade.Int64
		}
		if gradeComment.Valid {
			t.GradeComment = gradeComment.String
		}
		if finishDate.Valid {
			parsedDate, err := time.Parse(model.TicketDatesFromDbLayout, finishDate.String)
			if err == nil {
				t.FinishDate = &parsedDate
			}
		}
		if inn.Valid {
			t.Inn = inn.String
		}
		for _, iu := range imageUrls {
			if iu.Valid {
				t.ImageUrls = append(t.ImageUrls, iu.String)
			}
		}
		for _, du := range documentUrls {
			if du.Valid {
				t.DocumentUrls = append(t.DocumentUrls, du.String)
			}
		}
		for _, vu := range videoUrls {
			if vu.Valid {
				t.VideoUrls = append(t.VideoUrls, vu.String)
			}
		}
	}
	return &t, nil
}

func (r *PGXRepository) GetFirstNewTicketForManagerByTicketTypeOrWithoutTicketType(ctx context.Context, excludedIds []int64, ticketTypesId ...int64) (*model.Ticket, error) {
	var ticketTypeIdSql []sql.NullInt64

	if len(ticketTypesId) == 1 && ticketTypesId[0] == 0 {
		ticketTypesId = nil
	}

	for _, id := range ticketTypesId {
		ticketTypeIdSql = append(ticketTypeIdSql, sql.NullInt64{Int64: id, Valid: true})
	}

	var excludedIdsSql []sql.NullInt64
	for _, ei := range excludedIds {
		excludedIdsSql = append(excludedIdsSql, sql.NullInt64{Int64: ei, Valid: true})
	}

	query := `select t.id, t.pickpoint_id, t.user_id, pp.address, pp.pickpoint_id, pp.phone, pp.user_name, 
       			mg.grade, mg.note , t.create_date, t.finish_date, t.description, t.ticket_type, tt.description as ticket_type_name, 
       			t.ticket_state, t.image_urls, t.open_after_answer, t.is_read, t.video_urls, t.document_urls, 
       			t.manager_id, t.update_date, po.inn
		      from tickets t
		          left join tickets_types tt ON  tt.id=t.ticket_type
				  left join managers_grades mg on mg.ticket_id=t.id and mg.manager_id = t.manager_id
				  left join managers m_ticket_creator on m_ticket_creator.user_id = t.user_id
				  left join managers_pickpoints_info pp on
					 pp.manager_id = m_ticket_creator.id
				     and pp.pickpoint_id=t.pickpoint_id
				  left join managers_pickpoints_info pp_owner on t.pickpoint_id = pp_owner.pickpoint_id and pp_owner.is_owner = true
				  left join managers pp_owner_user_id on pp_owner_user_id.id = pp_owner.manager_id
				  left join pickpoint_owner po on po.user_id = pp_owner_user_id.user_id
	 		   where ($1::bigint[] is null and t.ticket_type not in (8, 28, 23, 30, 32, 19, 33, 34, 35, 36, 31, 39, 10, 9, 46)) or (t.ticket_type = any ($1))
	 		      and t.ticket_state in (1,2)
 	 			  and t.manager_id = 0
	  		      and ($2::bigint[] is null or not (t.id = any ($2)))
			  order by t.ticket_state desc,t.open_after_answer desc, t.create_date, t.id
	 		  limit 1`

	rows, err := r.dbSlave.Query(ctx, query, pq.Array(ticketTypeIdSql), pq.Array(excludedIdsSql))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &model.Ticket{}, nil
		}
		return &model.Ticket{}, err
	}
	t := model.Ticket{}
	t.PickPointInfo = &model.PickPointInfo{}
	var (
		ppInfoAddress     sql.NullString
		ppInfoPhone       sql.NullInt64
		ppInfoName        sql.NullString
		ppInfoPickPointId sql.NullInt64
		ticketGrade       sql.NullInt64
		gradeComment      sql.NullString
		finishDate        sql.NullString
		imageUrls         []sql.NullString
		videoUrls         []sql.NullString
		documentUrls      []sql.NullString
		inn               sql.NullString
	)

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&t.Id, &t.PickPointId, &t.UserId, &ppInfoAddress, &ppInfoPickPointId, &ppInfoPhone, &ppInfoName, &ticketGrade, &gradeComment,
			&t.CreateDate, &finishDate, &t.Description, &t.TicketTypeId, &t.TicketTypeName, &t.TicketStateId, pq.Array(&imageUrls), &t.OpenAfterAnswer, &t.IsRead,
			pq.Array(&videoUrls), pq.Array(&documentUrls), &t.ManagerId, &t.UpdateDate, &inn)
		if err != nil {
			return &model.Ticket{}, err
		}
		if ppInfoAddress.Valid {
			t.PickPointInfo.Address = ppInfoAddress.String
		}
		if ppInfoPhone.Valid {
			t.PickPointInfo.Phone = ppInfoPhone.Int64
		}
		if ppInfoName.Valid {
			t.PickPointInfo.Name = ppInfoName.String
		}
		if ppInfoPickPointId.Valid {
			t.PickPointInfo.PickPointId = ppInfoPickPointId.Int64
		}
		if ticketGrade.Valid {
			t.TicketGrade = ticketGrade.Int64
		}
		if gradeComment.Valid {
			t.GradeComment = gradeComment.String
		}
		if finishDate.Valid {
			parsedDate, err := time.Parse(model.TicketDatesFromDbLayout, finishDate.String)
			if err == nil {
				t.FinishDate = &parsedDate
			}
		}
		if inn.Valid {
			t.Inn = inn.String
		}
		for _, iu := range imageUrls {
			if iu.Valid {
				t.ImageUrls = append(t.ImageUrls, iu.String)
			}
		}
		for _, du := range documentUrls {
			if du.Valid {
				t.DocumentUrls = append(t.DocumentUrls, du.String)
			}
		}
		for _, vu := range videoUrls {
			if vu.Valid {
				t.VideoUrls = append(t.VideoUrls, vu.String)
			}
		}
	}
	return &t, nil
}

func (r *PGXRepository) GetTicketHistory(ctx context.Context, ticketIds []int64) (map[int64][]*model.TicketHistory, error) {
	query := `select 
    			coalesce((select m.user_id from managers m where m.id = th.manager_id), -1), 
    			th.fio, 
    			th.create_date, 
    			th.state,
    			ts.name, 
    			th.ticket_id 
			  from tickets_history th
			  	left join tickets_states ts on th.state = ts.id 
			  where th.ticket_id = any ($1)
 			  order by th.id`

	rows, err := r.dbSlave.Query(ctx, query, ticketIds)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := make(map[int64][]*model.TicketHistory)
	for rows.Next() {
		var fio sql.NullString
		th := model.TicketHistory{}
		err := rows.Scan(&th.UserId, &fio, &th.CreateDate, &th.State, &th.StateName, &th.TicketId)
		if err != nil {
			if err == pgx.ErrNoRows {
				return map[int64][]*model.TicketHistory{}, nil
			}
			return map[int64][]*model.TicketHistory{},
				fmt.Errorf("[GetTicketHistory] failed to get history: %w", err)
		}
		if fio.Valid {
			th.Fio = fio.String
		}
		arr := result[th.TicketId]
		arr = append(arr, &th)
		result[th.TicketId] = arr
	}

	return result, nil
}

func (r *PGXRepository) GetUserIdFromTicket(ctx context.Context, ticketId int64) (int64, error) {
	query := `select user_id from tickets where id = $1`

	var userId int64
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&userId)
	if err != nil {
		return 0, err
	}
	return userId, nil
}

func (r *PGXRepository) SetTicketIsRead(ctx context.Context, isRead bool, ticketId int64) error {
	query := `update tickets set is_read = $1 where id = $2`

	_, err := r.db.Exec(ctx, query, isRead, ticketId)
	if err != nil {
		return err
	}
	return nil
}

func (r *PGXRepository) SetReOpenAfterAnswer(ctx context.Context, ticketId, openAfterAnswer int64) error {

	query := `update tickets set open_after_answer=$1 where id=$2;`
	_, err := r.db.Exec(ctx, query, openAfterAnswer, ticketId)
	if err != nil {
		return err
	}
	return nil
}

func (r *PGXRepository) IsTicketExist(ctx context.Context, ticketId int64) (bool, error) {
	query := `select exists(select * from tickets where id = $1)`

	var ticketExist bool
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&ticketExist)
	if err != nil {
		return false, err
	}

	return ticketExist, nil
}

func (r *PGXRepository) SetTicketAsAutoClosed(ctx context.Context, ticketId int64) error {
	query := `update tickets SET auto_closed = 1, ticket_state = 3 WHERE id=$1`

	_, err := r.db.Exec(ctx, query, ticketId)
	if err != nil {
		return err
	}

	return nil
}

func (r *PGXRepository) GetTicketIdByShk(ctx context.Context, shk int64) (int64, error) {
	query := `select og.ticket_id from overdue_goods og 
			left join tickets t on t.id=og.ticket_id
	where og.shk = $1 and t.ticket_state != 3;`

	var ticketId int64
	rows, err := r.dbSlave.Query(ctx, query, shk)
	if err != nil {
		return 0, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&ticketId)
		if err != nil {
			return 0, err
		}
	}

	return ticketId, err
}

func (r *PGXRepository) GetPickpointIdByTicketId(ctx context.Context, ticketId int64) (int64, error) {
	query := `select t.pickpoint_id from tickets t where id = $1;`

	var pickpointId int64
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&pickpointId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return pickpointId, err
}

func (r *PGXRepository) GetPickpointOrderIdByTicketId(ctx context.Context, ticketId int64) (int64, error) {
	query := `select t.pickpoint_order_id from tickets t where id = $1;`

	var pickpointId int64
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&pickpointId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return pickpointId, err
}

func (r *PGXRepository) GetTicketIdByReplacedShk(ctx context.Context, shk int64) (int64, error) {
	query := `select rg.ticket_id from replaced_goods rg 
			left join tickets t on t.id=rg.ticket_id
	where rg.shk = $1 and t.ticket_state != 3;`

	var ticketId int64
	rows, err := r.dbSlave.Query(ctx, query, shk)
	if err != nil {
		return 0, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&ticketId)
		if err != nil {
			return 0, err
		}
	}

	return ticketId, err
}

func (r *PGXRepository) UpdateTicket(ctx context.Context, ticketId int64, description string, imageUrls, videoUrls, documentUrls []string) error {

	var (
		descriptionSql  sql.NullString
		imageUrlsSql    []sql.NullString
		videoUrlsSql    []sql.NullString
		documentUrlsSql []sql.NullString
	)

	if len(description) > 0 {
		descriptionSql = sql.NullString{String: description, Valid: true}
	}

	for _, iu := range imageUrls {
		imageUrlsSql = append(imageUrlsSql, sql.NullString{String: iu, Valid: true})
	}

	for _, vu := range videoUrls {
		videoUrlsSql = append(videoUrlsSql, sql.NullString{String: vu, Valid: true})
	}

	for _, du := range documentUrls {
		documentUrlsSql = append(documentUrlsSql, sql.NullString{String: du, Valid: true})
	}

	query := `update tickets
			set description = coalesce($2, description),
				image_urls = coalesce($3, image_urls),
				video_urls = coalesce($4, video_urls),
				document_urls = coalesce($5, document_urls)
			where id = $1`

	_, err := r.db.Exec(ctx, query, ticketId, descriptionSql, pq.Array(imageUrlsSql), pq.Array(videoUrlsSql), pq.Array(documentUrlsSql))
	if err != nil {
		return err
	}

	return nil
}

func (r *PGXRepository) ResetManagerInTicket(ctx context.Context, ticketId int64) error {
	query := `update tickets set manager_id = 0 where id = $1`

	_, err := r.db.Exec(ctx, query, ticketId)
	if err != nil {
		return err
	}

	return nil
}

func (r *PGXRepository) GetAllSuspendedShkTicketIdsWithoutVideo(ctx context.Context) ([]int64, error) {
	query := `select og.ticket_id from (
                                     select distinct og.shk, og.ticket_id from overdue_goods og
                                                                                   left join tickets t on og.ticket_id = t.id
                                     where array_length(t.video_urls, 1) is null and array_length(t.image_urls, 1) is null
                                       and t.ticket_state !=3 and t.description not like '%https%') og
where not exists (select * from support_log sl where sl.ticket_id = og.ticket_id
                                                 and (array_length(sl.video_urls, 1)> 0 or (array_length(sl.image_urls, 1)> 0 and array_to_string(sl.image_urls, ',', '*') like '%video%')
        or sl.message like '%https%'))`

	var ticketIds []int64
	rows, err := r.dbSlave.Query(ctx, query)
	if err != nil {
		if err == pgx.ErrNoRows {
			return []int64{}, nil
		}
		return []int64{}, err
	}

	defer rows.Close()
	for rows.Next() {
		var tId int64
		err := rows.Scan(&tId)
		if err != nil {
			return []int64{}, err
		}
		ticketIds = append(ticketIds, tId)
	}

	return ticketIds, nil
}

func (r *PGXRepository) GetBalanceTicketIdByTicketTypeAndTicketReasonAndUserId(ctx context.Context, userId int64) (int64, error) {
	query := `select t.id from tickets t 
              where t.ticket_type = 10 and t.ticket_reason_id in (11, 12, 13, 14)
                and t.user_id = $1 and round(cast(extract(epoch from ((now() at time zone ('utc'))- t.create_date)/86400) as numeric), 2) <= 7`

	var id int64
	err := r.dbSlave.QueryRow(ctx, query, userId).Scan(&id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return id, nil
}

func (r *PGXRepository) GetTicketsByKeyWordsAndTicketType(ctx context.Context, ticketType int64, keyWords []string) ([]int64, error) {
	query := `select t.id from tickets t where t.ticket_type=$1 and t.ticket_state = 1 and lower(t.description) ilike any($2) and ticket_reason_id != 99`

	rows, err := r.dbSlave.Query(ctx, query, ticketType, pq.Array(keyWords))
	if err != nil {
		return []int64{}, err
	}

	defer rows.Close()
	result := make([]int64, 0)
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return []int64{}, err
		}
		result = append(result, id)
	}
	return result, nil
}

func (r *PGXRepository) GetTicketsByTicketTypeAndTicketReason(ctx context.Context, ticketType int64, ticketReason int64) ([]int64, error) {
	query := `select t.id
	from tickets t
	where t.ticket_type = $1
	  and t.ticket_state = 1
	  and t.ticket_reason_id = $2`

	rows, err := r.dbSlave.Query(ctx, query, ticketType, ticketReason)
	if err != nil {
		return []int64{}, err
	}

	defer rows.Close()
	result := make([]int64, 0)
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return []int64{}, err
		}
		result = append(result, id)
	}
	return result, nil
}

func (r *PGXRepository) GetTicketsByKeyWordsAndTicketTypeWithExceptions(ctx context.Context, ticketType int64, keyWords []string, exceptions []string) ([]int64, error) {
	query := `select t.id from tickets t where t.ticket_type=$1 and t.ticket_state = 1 and lower(t.description) ilike any($2) and lower(description) not ilike any($3)`

	rows, err := r.dbSlave.Query(ctx, query, ticketType, pq.Array(keyWords), pq.Array(exceptions))
	if err != nil {
		return []int64{}, err
	}

	defer rows.Close()
	result := make([]int64, 0)
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return []int64{}, err
		}
		result = append(result, id)
	}
	return result, nil
}

func (r *PGXRepository) GetTicketsCountByTicketState(ctx context.Context, ticketState model.TicketStateId, ticketType int64) (int64, error) {
	query := `select count(*)
	from tickets t 
	where t.ticket_state = $1 and t.ticket_type not in (8, 28, 23, 30, 32, 19, 33, 34, 35, 39, 10, 9) %s`

	addQuery := ""
	params := make([]interface{}, 0, 2)
	params = append(params, ticketState)
	if ticketType > 0 {
		addQuery = addQuery + " and t.ticket_type = $2"
		params = append(params, ticketType)
	}

	var ticketsCount int64
	err := r.dbSlave.QueryRow(ctx, fmt.Sprintf(query, addQuery), params...).Scan(&ticketsCount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return -1, err
	}
	return ticketsCount, nil
}

func (r *PGXRepository) GetClosedDailyTickets(ctx context.Context, userId int64) (int64, error) {
	query := `select count(*) from tickets t left join managers m on m.id = t.manager_id where (t.finish_date + interval '3 hours')::date = date($1) and m.user_id = $2;`
	date := time.Now()
	var closedTicketsCount int64
	err := r.dbSlave.QueryRow(ctx, query, date, userId).Scan(&closedTicketsCount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return closedTicketsCount, nil
}

func (r *PGXRepository) GetTechnicalProblemTicketsByPickPointID(ctx context.Context, pickPointId int64) (int64, error) {
	query := `select count(id) from tickets t where t.ticket_type = 2 and t.pickpoint_id = $1 and t.create_date + interval '7 days' > current_date;`
	var technicalTicketsCount int64
	err := r.dbSlave.QueryRow(ctx, query, pickPointId).Scan(&technicalTicketsCount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return technicalTicketsCount, nil
}

func (r *PGXRepository) GetPickPointOwnerUserIDByTicketid(ctx context.Context, ticketId int64) (int64, error) {
	query := `select m.user_id
	from managers m
			 left join managers_pickpoints_info mpi on m.id = mpi.manager_id and mpi.is_owner = true
			 left join tickets t on t.pickpoint_id = mpi.pickpoint_id
	where t.id = $1;`
	var ownerUserId int64
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&ownerUserId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return ownerUserId, nil
}

func (r *PGXRepository) GetPickpointDeliveryTicketsByPickPointIDAndTicketReasonID(ctx context.Context, pickPointId, ticketReasonId int64) (int64, error) {
	query := `select count(id) 
	from tickets t 
	where t.ticket_type = 31
	and t.ticket_reason_id = $1 
	and t.pickpoint_id = $2
	and current_date < t.create_date + 
		CASE 
			WHEN $1 = $3 THEN interval '3 days' 
			ELSE interval '1 day' 
		END;`
	var changeOwnerTicketsCount int64
	err := r.dbSlave.QueryRow(ctx, query, ticketReasonId, pickPointId, model.TicketReasonLimitShk).Scan(&changeOwnerTicketsCount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return changeOwnerTicketsCount, nil
}

func (r *PGXRepository) GetPreviousSubsidyTicketCreatedByPickpointID(ctx context.Context, pickPointId int64) (date time.Time, err error) {
	query := `select create_date
	from tickets t 
	where t.ticket_type = $1
	and t.pickpoint_id = $2
	order by create_date desc
	limit 1`

	err = r.dbSlave.QueryRow(ctx, query, model.SubsidyTicketTypeId, pickPointId).Scan(&date)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, nil
		}
		return time.Now(), err
	}
	return date, nil
}

func (r *PGXRepository) InsertIntoChangeOwnerInfo(ctx context.Context, ticketId int64, changeOwnerInfo model.ChangeOwnerInfo) error {
	query := `insert into change_owner_info (ticket_id, old_owner_phone, new_owner_phone, new_owner_inn, new_owner_fio, internal_pickpoint_id)
	values ($1, $2, $3, $4, $5, $6)`

	_, err := r.db.Exec(ctx, query, ticketId, changeOwnerInfo.OldOwnerPhone, changeOwnerInfo.NewOwnerPhone, changeOwnerInfo.NewOwnerInn, changeOwnerInfo.NewOwnerFio, changeOwnerInfo.InternalPickpointId)
	if err != nil {
		return err
	}

	return nil
}

func (r *PGXRepository) InsertIntoChangeOwnerUser(ctx context.Context, ticketId int64, changeOwnerUser model.ChangeOwnerUser) error {
	query := `insert into change_owner_users (ticket_id, old_owner_user_id, new_owner_user_id, internal_pickpoint_id)
	values ($1, $2, $3, $4)`

	_, err := r.db.Exec(ctx, query, ticketId, changeOwnerUser.OldOwnerUser, changeOwnerUser.NewOwnerUser, changeOwnerUser.InternalPickpointId)
	if err != nil {
		return err
	}

	return nil
}

func (r *PGXRepository) ExistCheckTicketChangeOwnerType(ctx context.Context, ppId int64) (exist bool, err error) {
	query := `select exists(select true from tickets where pickpoint_id = $1 and ticket_type = $2 and ticket_state != $3);`

	err = r.dbSlave.QueryRow(ctx, query, ppId, model.ChangeOwnerTicketTypeId, model.ClosedTicketState).Scan(&exist)
	if err != nil {
		return false, err
	}
	return exist, nil
}

func (r *PGXRepository) GetChangeOwnerInfo(ctx context.Context, ticketId int64) (model.ChangeOwnerInfo, error) {
	query := `select coi.old_owner_phone, coi.new_owner_phone, coi.new_owner_inn, coi.new_owner_fio, coi.internal_pickpoint_id from change_owner_info coi
	where ticket_id = $1`

	changeOwnerInfo := model.ChangeOwnerInfo{}
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&changeOwnerInfo.OldOwnerPhone, &changeOwnerInfo.NewOwnerPhone, &changeOwnerInfo.NewOwnerInn, &changeOwnerInfo.NewOwnerFio, &changeOwnerInfo.InternalPickpointId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return model.ChangeOwnerInfo{}, nil
		}
		return model.ChangeOwnerInfo{}, err
	}
	return changeOwnerInfo, nil
}

func (r *PGXRepository) GetTicketsPickpointList(ctx context.Context, day string) ([]int64, error) {
	query := `SELECT DISTINCT pickpoint_id
	FROM tickets
	WHERE DATE(create_date) = $1`

	rows, err := r.dbSlave.Query(ctx, query, day)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	list := make([]int64, 0)
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		list = append(list, id)
	}
	return list, rows.Err()
}

func (r *PGXRepository) GetTicketsStat(ctx context.Context, day string, pickpointIDs []int64) ([]*model.TicketStat, error) {
	query := `SELECT t.user_id, t.pickpoint_id, t.ticket_type, count(t.id), count(th.id)
	FROM tickets t
	LEFT JOIN tickets_history th ON th.ticket_id = t.id AND th.state = $1
	WHERE DATE(t.create_date) = $2
	AND pickpoint_id = ANY ($3)
	GROUP BY t.user_id, t.ticket_type, t.pickpoint_id`

	rows, err := r.dbSlave.Query(ctx, query, model.RefundTicketState, day, pq.Array(pickpointIDs))
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	list := make([]*model.TicketStat, 0)
	for rows.Next() {
		stat := new(model.TicketStat)
		err = rows.Scan(&stat.UserID, &stat.PickpointID, &stat.TicketType, &stat.Count, &stat.Refund)
		if err != nil {
			return nil, err
		}
		list = append(list, stat)
	}
	return list, rows.Err()
}

func (r *PGXRepository) GetChangeOwnerUser(ctx context.Context, ticketId int64) (model.ChangeOwnerUser, error) {
	query := `select cou.old_owner_user_id, cou.new_owner_user_id, cou.internal_pickpoint_id from change_owner_users cou
	where cou.ticket_id = $1`

	changeOwnerUser := model.ChangeOwnerUser{}
	err := r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&changeOwnerUser.OldOwnerUser, &changeOwnerUser.NewOwnerUser, &changeOwnerUser.InternalPickpointId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return model.ChangeOwnerUser{}, nil
		}
		return model.ChangeOwnerUser{}, err
	}
	return changeOwnerUser, nil
}

func (r *PGXRepository) ReopenTickets(ctx context.Context, ticketIds []int64, userId int64) error {
	managerId, err := r.GetManagerId(ctx, userId)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	batch := &pgx.Batch{}
	batch.Queue(`delete from tickets_history where ticket_id = any($1) and state=3;`, ticketIds)
	batch.Queue(`update tickets set ticket_state = 2, manager_id = $1 where id = any($2);`, managerId, ticketIds)
	batch.Queue(`update replaced_goods set deleted = null where ticket_id = any($1);`, ticketIds)
	batch.Queue(`update overdue_goods set deleted = null where ticket_id = any($1);`, ticketIds)

	br := r.db.SendBatch(ctx, batch)
	defer br.Close()
	_, err = br.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (r *PGXRepository) GetOldTickets(ctx context.Context) ([]*model.Ticket, error) {
	query := `select
    t.id,
    t.user_id,
    t.create_date,
    t.finish_date,
    t.description,
    t.ticket_type,
    t.ticket_state,
    t.image_urls,
    t.pickpoint_id,
    t.manager_id,
    t.open_after_answer,
    t.is_read,
    t.video_urls,
    t.document_urls,
    t.auto_closed,
    t.update_date,
    t.ticket_reason_id,
    coalesce(t.app_source, 0),
    coalesce(t.app_version,'')
        from tickets t inner join (select ticket_id, max(create_date) as max_date from tickets_history
            where state = 3 group by ticket_id) th on t.id = th.ticket_id
            where t.ticket_state = 3
              and t.finish_date < now()-interval '3 month'
                and th.max_date < now()-interval '3 month';`
	rows, err := r.dbSlave.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tickets []*model.Ticket
	for rows.Next() {
		var t model.Ticket
		err = rows.Scan(&t.Id, &t.UserId, &t.CreateDate, &t.FinishDate, &t.Description, &t.TicketTypeId, &t.TicketStateId, &t.ImageUrls,
			&t.PickPointId, &t.ManagerId, &t.OpenAfterAnswer, &t.IsRead, &t.VideoUrls, &t.DocumentUrls, &t.AutoClosed, &t.UpdateDate,
			&t.TicketReasonId, &t.AppSource, &t.AppVersion)
		if err != nil {
			return nil, err
		}
		tickets = append(tickets, &t)
	}

	return tickets, nil
}

func (r *PGXRepository) MoveTicketToTicketsArchive(ctx context.Context, t *model.Ticket) error {
	queryInsertInArchive := `insert into tickets_archive (id,
    user_id, create_date, finish_date, description,
    ticket_type, ticket_state, image_urls, pickpoint_id,
    manager_id, open_after_answer, is_read, video_urls,
    document_urls, auto_closed, update_date, ticket_reason_id,
    app_source, app_version) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19);`

	queryRemoveFromTickets := `delete from tickets where id = $1;`

	batch := &pgx.Batch{}

	batch.Queue(queryInsertInArchive, t.Id, t.UserId, t.CreateDate, t.FinishDate, t.Description, t.TicketTypeId, t.TicketStateId, t.ImageUrls,
		t.PickPointId, t.ManagerId, t.OpenAfterAnswer, t.IsRead, t.VideoUrls, t.DocumentUrls, t.AutoClosed, t.UpdateDate,
		t.TicketReasonId, t.AppSource, t.AppVersion)

	batch.Queue(queryRemoveFromTickets, t.Id)

	br := r.db.SendBatch(ctx, batch)
	defer br.Close()

	_, err := br.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (r *PGXRepository) HasDaysPassedSinceLastTicketWithTicketTypeId(ctx context.Context, pickPointId, ticketTypeId int64, passedTime time.Duration) (ticketId int64, err error) {

	query := `select id from tickets where ticket_type = $1
                        and ticket_state <= 3
                        and (finish_date > $2 or finish_date is NULL)
                        and pickpoint_id = $3 limit 1;`

	timeAgo := time.Now().Add(-passedTime)
	err = r.dbSlave.QueryRow(ctx, query, ticketTypeId, timeAgo, pickPointId).Scan(&ticketId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return ticketId, nil
}

func (r *PGXRepository) TicketExistFromTicketIds(ctx context.Context, ticketIds []int64, ticketTypeId, userId, pickpointId int64) (ticketId int64, err error) {
	query := `SELECT id FROM tickets WHERE id = ANY($1) AND ticket_state IN (1,2) AND ticket_type = $2 AND user_id = $3 AND pickpoint_id = $4 LIMIT 1;`
	err = r.dbSlave.QueryRow(ctx, query, ticketIds, ticketTypeId, userId, pickpointId).Scan(&ticketId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return ticketId, nil
}

func (r *PGXRepository) GetTicketsForCC(ctx context.Context, filter *model.TicketFilter) (tickets []*model.Ticket, err error) {
	var opts []string

	query := `SELECT t.id, t.ticket_type, t.create_date, t.finish_date,coalesce((select m.user_id from managers m where m.id=t.manager_id),0), coalesce(mg.grade,0)
					FROM tickets t
					    left join managers_grades mg on mg.ticket_id=t.id and mg.manager_id = t.manager_id`

	if filter != nil {

		if len(filter.TicketTypeIds) > 0 {
			var ticketTypes []string
			for _, tt := range filter.TicketTypeIds {
				ticketTypes = append(ticketTypes, fmt.Sprint(tt))
			}
			opts = append(opts, fmt.Sprintf("t.ticket_type in (%s)", strings.Join(ticketTypes, ", ")))
		}

		if filter.DateFrom != "" && filter.DateTo != "" {
			var opt string
			if model.TicketStateId(filter.TicketStateId) == model.ClosedTicketState {
				opt = fmt.Sprintf("t.finish_date >= '%s' and t.finish_date <= '%s'", filter.DateFrom, filter.DateTo)
			} else {
				opt = fmt.Sprintf("t.create_date >= '%s' and t.create_date <= '%s'", filter.DateFrom, filter.DateTo)
			}
			opts = append(opts, opt)
		}

		if len(opts) != 0 {
			where := fmt.Sprintf(" where %s", strings.Join(opts, " and "))
			query += where
		}

		query += " order by t.finish_date "

		if filter.Limit > 0 {
			limit := fmt.Sprintf(" limit %v", filter.Limit)
			query += limit
		}
	}

	rows, err := r.dbSlave.Query(ctx, query)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			logger.Error(ctx, err.Error())
			return nil, nil
		}
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var t model.Ticket
		err = rows.Scan(&t.Id, &t.TicketTypeId, &t.CreateDate, &t.FinishDate, &t.ManagersUserId, &t.TicketGrade)
		if err != nil {
			return nil, err
		}
		tickets = append(tickets, &t)
	}

	return tickets, rows.Err()
}

func (r *PGXRepository) IsTicketTariffsLessThenMonthExist(ctx context.Context, userId int64, pickpointId int64) (bool, error) {
	monthAgo := time.Now().AddDate(0, -1, 0)
	timeForTariffEmergency := time.Date(2023, 9, 22, 12, 0, 0, 0, time.Local)

	query := `select exists(select id from tickets where
			  	user_id = $1 and ticket_type = $2 and pickpoint_id = $3 and create_date > $4 and create_date > $5)`

	var exists bool
	err := r.dbSlave.QueryRow(ctx, query, userId, model.Tariffs, pickpointId, monthAgo, timeForTariffEmergency).Scan(&exists)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return exists, nil
}

func (r *PGXRepository) GetShksByTicketIdForReplacedOrOverdue(ctx context.Context, ticketId, ticketType int64) (shks []int64, err error) {
	query := `select shk from replaced_goods where ticket_id = $1;`
	if ticketType != model.ReplacedGoods {
		query = `select shk from overdue_goods where ticket_id = $1;`
	}

	rows, err := r.dbSlave.Query(ctx, query, ticketId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var shk int64
		err = rows.Scan(&shk)
		if err != nil {
			return nil, err
		}
		shks = append(shks, shk)
	}
	return shks, rows.Err()
}

func (r *PGXRepository) GetIsPushSentForExpensiveTicket(ctx context.Context, ticketId int64) (isSent bool, err error) {
	query := `select exists(select is_push_sent from expensive_goods_pushes_sent where ticket_id = $1)`
	err = r.dbSlave.QueryRow(ctx, query, ticketId).Scan(&isSent)
	return
}

func (r *PGXRepository) SetIsPushSentForExpensiveTicket(ctx context.Context, ticketId int64) (err error) {
	query := `insert into expensive_goods_pushes_sent (ticket_id, is_push_sent) values ($1, true) on conflict (ticket_id) do update set is_push_sent = true`
	_, err = r.db.Exec(ctx, query, ticketId)
	return
}
