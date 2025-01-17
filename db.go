package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var (
	schema = []string{
		"CREATE TABLE `hashes`(`id` Integer NOT NULL PRIMARY KEY, `hash` Text NOT NULL, `divider` Real NOT NULL, CONSTRAINT `unique_id` UNIQUE (`id`), CONSTRAINT `unique_hash_divider` UNIQUE (`hash` COLLATE NOCASE, `divider`))",
		"CREATE UNIQUE INDEX `hash_divider` ON `hashes`(`hash` COLLATE NOCASE, `divider`)",
		"CREATE TABLE `pools`(`id` Integer NOT NULL PRIMARY KEY AUTOINCREMENT, `host` Text NOT NULL COLLATE NOCASE, `port` Integer NOT NULL, `hash_id` Integer NOT NULL, CONSTRAINT `unique_id` UNIQUE (`id`), CONSTRAINT `unique_host_port` UNIQUE (`host` COLLATE NOCASE, `port`), CONSTRAINT `lnk_hashes_pools` FOREIGN KEY (`hash_id`) REFERENCES `hashes`(`id`) ON DELETE Cascade ON UPDATE Cascade)",
		"CREATE UNIQUE INDEX `host_port` ON `pools`(`host` COLLATE NOCASE, `port`)",
		"CREATE TABLE `users`(`name` Text NOT NULL PRIMARY KEY COLLATE NOCASE, `pool_id` Integer NOT NULL, `user` Text NOT NULL, `password` Text NOT NULL, CONSTRAINT `unique_name` UNIQUE (`name` COLLATE NOCASE), CONSTRAINT `unique_pool_user_password` UNIQUE (`pool_id`, `user` COLLATE NOCASE, `password` COLLATE NOCASE), CONSTRAINT `lnk_pools_users` FOREIGN KEY (`pool_id`) REFERENCES `pools`(`id`) ON DELETE Cascade ON UPDATE Cascade)",
		"CREATE UNIQUE INDEX `pool_user_password` ON `users`(`pool_id`, `user` COLLATE NOCASE, `password` COLLATE NOCASE)",
	}
	data = []string{
		"INSERT INTO `hashes` (`hash`, `divider`) VALUES ('sha256', 1.0)",
		"INSERT INTO `pools` (`host`, `port`, `hash_id`) VALUES ('ss.antpool.com', 3333, (SELECT `id` FROM `hashes` WHERE `hash` = 'sha256'))",
	}
)

/*
Db - the database of proxy.
*/
type Db struct {
	handle *sql.DB
}

/*
Init - initializing of the database. Validating of the existence of the database.
If the database is not exist we are creating a database and filling it of default values.
If the database is exist and correct we are opening connect to it.

@return bool if initializing successfull.
*/
func (d *Db) Init() bool {
	LogInfo("proxy : check database file on path %s", "", dbPath)
	_, err := os.Stat(dbPath)
	if os.IsPermission(err) {
		LogError("proxy : access denied to %s", "", dbPath)
		return false
	}
	if os.IsNotExist(err) {
		LogInfo("proxy : database file not exist", "")
		d.Create()
	}

	LogInfo("proxy : opening database file on path %s", "", dbPath)
	d.handle, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		LogError("proxy : error opening database on path %s: %s", "", dbPath, err.Error())
		return false
	}

	return true
}

/*
Create - the creating the database and filling it of default values.

@return error
*/
func (d *Db) Create() error {
	LogInfo("proxy : creating database on path %s", "", dbPath)
	td, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		LogError("proxy : error creating database on path %s: %s", "", dbPath, err.Error())
		return err
	}
	defer td.Close()
	t, err := td.Begin()
	if err != nil {
		LogError("proxy : error starting transaction: %s", "", err.Error())
		return err
	}
	LogInfo("proxy : creating schema", "")
	for _, v := range schema {
		if _, err := td.Exec(v); err != nil {
			LogError("proxy : error executing query %s: %s", "", v, err.Error())
			t.Rollback()
			return err
		}
	}
	LogInfo("proxy : loading default data", "")
	for _, v := range data {
		if _, err := td.Exec(v); err != nil {
			LogError("proxy : error executing query %s: %s", "", v, err.Error())
			t.Rollback()
			return err
		}
	}
	t.Commit()

	return nil
}

/*
Close - the closing of the database.
*/
func (d *Db) Close() {
	LogInfo("proxy : closing database file on path %s", "", dbPath)
	_ = d.handle.Close()
}

/*
AddUser - the addding of the user to the database.

@param *User user pointer to User struct.

@return error
*/
func (d *Db) AddUser(user *User) error {
	id, err := d.GetPool(user.pool)
	if err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("pool %s not found", user.pool)
	}
	if _, err := d.handle.Exec(
		"INSERT INTO `users` (`name`, `pool_id`, `user`, `password`) VALUES ($1, $2, $3, $4);",
		user.name,
		id,
		user.user,
		user.password,
	); err != nil {
		return err
	}

	return nil
}

/*
GetUser - the getting of the user from the database.

@param string name user name.

@return *User the pointer to the founded user.

	error
*/
func (d *Db) GetUser(name string) (*User, error) {
	var host string
	var port int

	user := new(User)

	row := d.handle.QueryRow("SELECT `u`.`name`, `p`.`host`, `p`.`port`, `u`.`user`, `u`.`password`, `h`.`hash`, `h`.`divider` FROM `users` AS `u` INNER JOIN `pools` AS `p` ON `u`.`pool_id` = `p`.`id` INNER JOIN `hashes` AS `h` ON `p`.`hash_id` = `h`.`id` WHERE `u`.`user` = $1;", name)
	err := row.Scan(&user.name, &host, &port, &user.user, &user.password, &user.hash, &user.divider)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user with name = %s not found", name)
	}
	if err != nil {
		return nil, err
	}
	user.pool = host + ":" + strconv.Itoa(port)

	return user, nil
}

/*
GetUserByPool - the getting of the user by the pool and the user name of the pool.

@param string pool the user pool in format addr:port.
@param string user the name of the user.

@return *User the pointer to the founded user.

	error
*/
func (d *Db) GetUserByPool(pool string, user string) (*User, error) {
	id, err := d.GetPool(pool)
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, errors.New("pool not found")
	}
	if user == "" {
		return nil, errors.New("empty user")
	}

	us := User{pool: pool, user: user}

	row := d.handle.QueryRow("SELECT `u`.`name`, `u`.`password`, `h`.`hash`, `h`.`divider` FROM `users` AS `u` INNER JOIN `pools` AS `p` ON `u`.`pool_id` = `p`.`id` INNER JOIN `hashes` AS `h` ON `p`.`hash_id` = `h`.`id` WHERE `p`.`id` = $1 AND `u`.`user` = $2;", id, user)
	err = row.Scan(&us.name, &us.password, &us.hash, &us.divider)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &us, nil
}

/*
GetPool - the getting of the pool by his data.

@param string pool the pool of the user in format addr:port.

@return uint64 the identifier of the pool.

	error
*/
func (d *Db) GetPool(pool string) (uint64, error) {
	var id uint64

	if !ValidateAddr(pool, true) {
		return 0, fmt.Errorf("invalid format pool = %s", pool)
	}
	parts := strings.Split(pool, ":")
	port, _ := strconv.Atoi(parts[1])

	row := d.handle.QueryRow("SELECT `id` FROM `pools` WHERE `host` = $1 AND `port` = $2;", parts[0], port)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return id, nil
}
