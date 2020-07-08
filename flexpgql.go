package flexpgql

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
	"github.com/ariefdarmawan/datahub"
	"github.com/ariefdarmawan/flexpg"
)

// flexpgql Enchanment Dynamic Kaos mod db Response
type flexPGQLTable interface {
	TableName() string
}

type flexPGQLResponse struct {
	Data  interface{} `json:"data"`
	Count int         `json:"count"`
}

// Gets flexpgql Enchanment Dynamic Kaos mod db Response
func Gets(model orm.DataModel, h *datahub.Hub, parm *dbflex.QueryParam) (flexPGQLResponse, error) {
	rv := reflect.ValueOf(model)
	var mt reflect.Type
	if rv.Kind() == reflect.Ptr {
		mt = rv.Type().Elem()
	} else {
		mt = rv.Type()
	}
	mdl := reflect.New(mt).Interface().(orm.DataModel)
	dest := reflect.New(reflect.SliceOf(mt)).Interface()
	e := h.Gets(mdl, parm, dest)
	if e != nil {
		return flexPGQLResponse{}, e
	}
	// dont know, why it s not working
	// count, e := h.Count(mdl, parm)
	// m := toolkit.M{}.Set("data", dest).Set("count", count)
	// get count
	cmd := dbflex.From(mdl.TableName())
	if parm != nil && parm.Where != nil {
		cmd.Where(parm.Where)
	}
	cmd.Select("count(*) as RecordCount")
	mcount := struct {
		RecordCount int
	}{}
	conn, _ := h.GetClassicConnection()
	defer conn.Close()
	if e = conn.Cursor(cmd, nil).Fetch(&mcount).Close(); e != nil {
		return flexPGQLResponse{}, errors.New("could not get data count")
	}

	// m := toolkit.M{}.Set("data", dest).Set("count", mcount.RecordCount)
	return flexPGQLResponse{
		Data:  dest,
		Count: mcount.RecordCount,
	}, nil
}

// Populate returns all data based on table name and QueryParm
func Populate(h *datahub.Hub, model flexPGQLTable, parm *dbflex.QueryParam) (flexPGQLResponse, error) {
	rv := reflect.ValueOf(model)
	var mt reflect.Type
	if rv.Kind() == reflect.Ptr {
		mt = rv.Type().Elem()
	} else {
		mt = rv.Type()
	}
	mdl := reflect.New(mt).Interface().(orm.DataModel)
	dest := reflect.New(reflect.SliceOf(mt)).Interface()
	idx, conn, err := h.GetConnection()
	if err != nil {
		return flexPGQLResponse{}, fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.CloseConnection(idx, conn)
	qry := dbflex.From(mdl.TableName())
	qryCount := dbflex.From(mdl.TableName())
	if w := parm.Select; w != nil {
		qry.Select(w...)
	} else {
		qry.Select("*")
	}
	qryCount.Select("count(*) as RecordCount")
	if w := parm.Where; w != nil {
		qry.Where(w)
		qryCount.Where(w)
	}
	if o := parm.Sort; len(o) > 0 {
		qry.OrderBy(o...)
	}
	if o := parm.Skip; o > 0 {
		qry.Skip(o)
	}
	if o := parm.Take; o > 0 {
		qry.Take(o)
	}
	if o := parm.GroupBy; len(o) > 0 {
		qry.GroupBy(o...)
		qryCount.GroupBy(o...)
	}
	if o := parm.Aggregates; len(o) > 0 {
		qry.Aggr(o...)
	}
	cur := conn.Cursor(qry, nil)
	if err = cur.Error(); err != nil {
		return flexPGQLResponse{}, fmt.Errorf("error when running cursor for aggregation. %s", err.Error())
	}
	defer cur.Close()
	err = cur.Fetchs(dest, 0).Close()
	mcount := struct {
		RecordCount int
	}{}
	if e := conn.Cursor(qryCount, nil).Fetch(&mcount).Close(); e != nil {
		return flexPGQLResponse{}, fmt.Errorf("could not get data count. %s", e.Error())
	}
	// m := toolkit.M{}.Set("data", dest).Set("count", mcount.RecordCount)
	return flexPGQLResponse{
		Data:  dest,
		Count: mcount.RecordCount,
	}, nil
}

var (
	customGetsRegexString = regexp.MustCompile(`{{.([^{}]*)}}`)
	CustomGetsSQL         = "SELECT " +
		"{{." + dbflex.QuerySelect + "}} " +
		"{{." + dbflex.QueryWhere + "}} " +
		"{{." + dbflex.QueryOrder + "}} " +
		// "{{." + dbflex.QueryGroup + "}} " +
		"{{." + dbflex.QueryTake + "}} " +
		"{{." + dbflex.QuerySkip + "}} "
)

// CustomFromSQL indepedent select with select/where/order/take/skip from queryparam with model-view
func CustomFromSQL(h *datahub.Hub, fromsql string, parm flexpg.QueryParam, dest interface{}) (flexPGQLResponse, error) {
	sql := CustomGetsSQL
	sql = strings.ReplaceAll(sql, "{{."+dbflex.QuerySelect+"}}", parm.ToSelectFields()+" "+fromsql)
	sql = strings.ReplaceAll(sql, "{{."+dbflex.QueryWhere+"}}", parm.ToSQLWhere())
	sql = strings.ReplaceAll(sql, "{{."+dbflex.QueryOrder+"}}", parm.ToSQLSort())
	// sql = strings.ReplaceAll(sql, "{{."+dbflex.QueryGroup+"}}", parm.ToSQLGroup())
	sql = strings.ReplaceAll(sql, "{{."+dbflex.QueryTake+"}}", parm.ToSQLTake())
	sql = strings.ReplaceAll(sql, "{{."+dbflex.QuerySkip+"}}", parm.ToSQLSkip())

	countsql := CustomGetsSQL
	countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QuerySelect+"}}", "count(*) as RecordCount"+" "+fromsql)
	countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QueryWhere+"}}", parm.ToSQLWhere())
	countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QueryGroup+"}}", parm.ToSQLGroup())
	countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QueryOrder+"}}", "")
	// countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QueryGroup+"}}", "")
	countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QueryTake+"}}", "LIMIT 1")
	countsql = strings.ReplaceAll(countsql, "{{."+dbflex.QuerySkip+"}}", "")

	mcounts := []struct {
		RecordCount int
	}{}
	// h.Log().Debugf("flexpgql Custom SQL: %v", sql)
	if e := h.PopulateSQL(sql, dest); e != nil {
		return flexPGQLResponse{}, fmt.Errorf("fetchs error %s", e.Error())
	}
	// h.Log().Debugf("flexpgql Custom SQL: %v", countsql)
	if e := h.PopulateSQL(countsql, &mcounts); e != nil {
		return flexPGQLResponse{}, fmt.Errorf("fetchs error %s", e.Error())
	}
	count := 0
	if len(mcounts) > 0 {
		count = mcounts[0].RecordCount
	}
	// m := toolkit.M{}.Set("data", dest).Set("count", mcount.RecordCount)
	return flexPGQLResponse{
		Data:  dest,
		Count: count,
	}, nil
}
