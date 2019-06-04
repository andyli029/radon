/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */
package v1

import (
	"net/http"

	"config"
	"proxy"
	"router"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
	"fmt"
)

type attachParams struct {
	Name           string `json:"name"`
	Address        string `json:"address"`
	User           string `json:"user"`
	Password       string `json:"password"`
	MaxConnections int    `json:"max-connections"`
}

// AddAttachHandler impl.
func AddAttachHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		addAttachHandler(log, proxy, w, r)
	}
	return f
}

func initAttach(proxy *proxy.Proxy, backend string, log *xlog.Log) error {
	spanner := proxy.Spanner()
	router := proxy.Router()

	// create db from radon's router.
	tblList := router.Tables()
	for db, _ := range tblList {
		query := fmt.Sprintf("create database IF NOT EXISTS %s", db)
		_, err := spanner.ExecuteOnThisBackend(backend, query)
		if err != nil {
			log.Error("api.v1.add.backend.initBackend.error:%v", err)
			return err
		}
	}
	return nil
}

func addAttachHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	spanner := proxy.Spanner()
	route := proxy.Router()
	p := attachParams{}
	var err error
	routeDBs := make(map[string]struct{})

	err = r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.add.backend.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	conf := &config.BackendConfig{
		Name:           p.Name,
		Address:        p.Address,
		User:           p.User,
		Password:       p.Password,
		Charset:        "utf8",
		MaxConnections: p.MaxConnections,
		IsAttach:       true,
	}
	log.Warning("api.v1.add[from:%v].attach[%+v]", r.RemoteAddr, conf)
	err = scatter.Add(conf);
	if err != nil {
		log.Error("api.v1.add.attach[%+v].error:%+v", conf, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer func() {
		if err != nil {
			if err := scatter.Remove(conf); err != nil {
				log.Panic("api.v1.add.attach.Remove:%+v", err)
			}

			for db, _ := range routeDBs {
				if err := route.DropDatabase(db); err != nil {
					log.Panic("api.v1.add.attach.route.DropDB:%+v", err)
				}
			}

			return
		}

		if err := scatter.FlushConfig(); err != nil {
			log.Error("api.v1.add.backend.flush.config.error:%+v", err)
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}()

	// Sync table on the database on the attach node to radon's router as single table
	// just register the info on the radon, it wll be fast.
	backend := []string{conf.Name}
	dbQuery := "show databases"
	dbs, err := spanner.ExecuteOnThisBackend(conf.Name, dbQuery)
	if err != nil {
		log.Error("api.v1.add.attach[%+v].error:%+v", conf, err) //fixme
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, r := range dbs.Rows {
		db := string(r[0].Raw())

		if isSysDB := route.IsSystemDB(db); isSysDB {
			continue
		}

		err = route.CreateDatabase(db)
		if err != nil {
			log.Error("api.v1.add.attach[%+v].error:%+v", conf, err) //fixme
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		routeDBs[db] = struct{}{}

		tableQuery := fmt.Sprintf("show tables from %s", db)
		tables, err := spanner.ExecuteOnThisBackend(conf.Name, tableQuery)
		if err != nil {
			log.Error("api.v1.add.attach[%+v].error:%+v", conf, err) //fixme
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, r := range tables.Rows {
			table := string(r[0].Raw())
			err = route.CreateTable(db, table, "", router.TableTypeSingle, backend, nil)
			if err != nil {
				log.Error("api.v1.add.attach[%+v].error:%+v", conf, err) //fixme
				rest.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	err = initAttach(proxy, conf.Name, log)
	if err != nil {
		log.Error("api.v1.add.attach.Init.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//TODO: Sync mysql.user on the attach node to the Radon
}

// RemoveAttachHandler impl.
func RemoveAttachHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		removeAttachHandler(log, proxy, w, r)
	}
	return f
}

func removeAttachHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	backend := r.PathParam("name")
	conf := &config.BackendConfig{
		Name: backend,
	}
	log.Warning("api.v1.remove[from:%v].backend[%+v]", r.RemoteAddr, conf)

	if err := scatter.Remove(conf); err != nil {
		log.Error("api.v1.remove.backend[%+v].error:%+v", conf, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := scatter.FlushConfig(); err != nil {
		log.Error("api.v1.remove.backend.flush.config.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
