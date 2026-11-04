package mysql

import (
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"gorm.io/gorm"
)

// Verify the identity comparison and locking indexes, not just table names.
// Binary identities avoid case/accent folding and PAD SPACE collations.
func (s *Store) validateIndexes(db *gorm.DB) error {
	var indexes []struct {
		TableName, IndexName, Columns string
		NonUnique                     int
	}
	if err := db.Raw(`SELECT table_name,index_name,non_unique,GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS columns
 FROM information_schema.statistics WHERE table_schema=? AND table_name IN ('outbox','inbox') GROUP BY table_name,index_name,non_unique`, s.cfg.Database).Scan(&indexes).Error; err != nil {
		return err
	}
	expected := map[string]string{
		"outbox.PRIMARY": "namespace,tenant_id,message_id", "outbox.outbox_seq": "seq",
		"outbox.outbox_ready": "namespace,state,available_at,seq", "outbox.outbox_expired": "namespace,state,lease_until,seq",
		"outbox.outbox_terminal": "namespace,state,seq", "outbox.outbox_published_cleanup": "namespace,state,published_at,seq",
		"inbox.PRIMARY": "namespace,consumer,tenant_id,message_id", "inbox.inbox_seq": "seq", "inbox.inbox_cleanup": "namespace,processed_at,seq",
	}
	for _, idx := range indexes {
		key := idx.TableName + "." + idx.IndexName
		if columns, ok := expected[key]; ok {
			unique := idx.IndexName == "PRIMARY" || idx.IndexName == "outbox_seq" || idx.IndexName == "inbox_seq"
			if idx.Columns != columns || (idx.NonUnique == 0) != unique {
				return messaging.ErrSchema
			}
			delete(expected, key)
		} else if idx.NonUnique == 0 {
			return messaging.ErrSchema
		}
	}
	if len(expected) != 0 {
		return messaging.ErrSchema
	}
	var identities []struct {
		TableName, ColumnName, DataType string
		CharacterMaximumLength          int
	}
	if err := db.Raw(`SELECT table_name,column_name,data_type,character_maximum_length FROM information_schema.columns
 WHERE table_schema=? AND table_name IN ('outbox','inbox') AND column_name IN ('namespace','tenant_id','message_id','consumer')`, s.cfg.Database).Scan(&identities).Error; err != nil {
		return err
	}
	if len(identities) != 7 {
		return messaging.ErrSchema
	}
	for _, col := range identities {
		if col.DataType != "varbinary" || col.CharacterMaximumLength != 256 {
			return messaging.ErrSchema
		}
	}
	return nil
}
