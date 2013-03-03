;Sheira Ben Haim 
;Dvir Segal      

(ns project)
(use 'clojure.set )


;Initialize an empty set of tablesNames
(def tablesNames (ref {}))

;Initialize for every table a list of it columns
(def tablesColumns (ref {})) 

;Hash Map for operations
(def operations {"=" = "<" < ">" > "<=" <= ">=" >=})


;Create a table and add its name to tablesNames and tableColumns
(defn createTable [tableName] 
  "Gets a new table name and conjuncts it to the rest of the tablesNames and adds to tablesColumns a new empty list of its coloumns"
  (let [newTable(ref #{})]
    (let [newColList(ref nil)]
   (dosync (ref-set tablesNames(conj(deref tablesNames) {tableName newTable})))
   (dosync (ref-set tablesColumns(conj(deref tablesColumns){tableName newColList})))
   newTable))
  )

;Defines the columns for a given table
(defn defineColumns [table columns]
  "Gets columns and cocnat it to a table"
  (let [tableCols (get(deref tablesColumns)table)]
   (dosync (ref-set tableCols(concat(deref tableCols) columns )))
   columns))
  

;Finds a table by its name
(defn findTableIn [tableName, lookin]
  "returns a reference to a table by its name"
  (if (nil? (get (deref lookin) tableName))
    (throw (Exception. "Cannot find table"))
    (get (deref lookin) tableName)
    ))

;The core function for adding a new record in a given reference table
(defn insertRecord [tableReference newrecord]
  "Gets a record and insert it to a table"
  (dosync (alter tableReference conj newrecord))newrecord)


;gets table name and a record, check if all keys in record are valid, and adds the new record to table
;record should be a map
(defn insert [tblName record]
  "Adds a new record to a table "
  (let [ recordKeys (set (keys record))]
    (let [ tableCols (set (deref (get (deref tablesColumns) tblName)))]
      (let [ diff (difference recordKeys tableCols)]
        (if (not(empty? diff)) 
          (throw (Exception. "Cannot insert into table because one of the columns does not exist"))
          (insertRecord (findTableIn tblName tablesNames) record))
        diff)tableCols)recordKeys))
 
;Removes items from an existing set (table) by a given tablName & records to be removed
(defn deleteRecord [tblname recordsToRemove]
  "Removes an item for an existing set by a given record to be removed"
   (let [ tblRef (findTableIn tblname tablesNames) ]       
  (dosync (ref-set tblRef (disj (deref tblRef) recordsToRemove)))  
  (deref tblRef)))

;creates a new set by a cartesian multiplication of two sets
;Assuming there are maximum 2 tables to multiply
(defn cartesianMultiply [tbl1 tbl2]
  "creates a new set by a cartesian multiplication of two sets"  
  (let [result (ref #{})]
    (let [ref1 (findTableIn tbl1 tablesNames) ] 
      (let [ ref2 (findTableIn tbl2 tablesNames)]       
        (dosync (doseq [rec1 (deref ref1)] 
                  (doseq [rec2 (deref ref2)]
                    (insertRecord result (merge rec1 rec2)) 
                    )))))   
    (deref result)))

;Select relevant fields and returns them in a new table -  projection
(defn getSelected [tableData fieldsList]
 "Returns a new table with only the required fields for each record"  
   (let [result (ref #{})]
     (dosync (doseq [record tableData]
               (insertRecord result (select-keys record (into [] fieldsList)))))
     (deref result)))

;Updates a record in a table
(defn updateRecord [tableName filteredTable field value]
  "Updates a record in a table"
  (let [tableRef (findTableIn tableName tablesNames)]      
      (doseq [record (deref tableRef)]
        (if (contains? filteredTable record) 
          (dosync (let [newRecord (assoc record field value)]
            (deleteRecord tableName record)
            (insertRecord tableRef newRecord))       
          )))
      (deref tableRef)))



;Drops table from tableNames and remove its columns from tableColumns
(defn dropTable [tableName]
  "Drops table from tableNames and remove its columns from tableColumns"
  (dosync 
    (alter tablesColumns dissoc tableName)
      (alter tablesNames dissoc tableName)))

;KeepBrackets = 1 - each string in the list will be inserted to list with brackets chars
;KeppBrackets = 0 - each string in the list will be inserted to list without brackets chars
(defn ParseByBarckets [conditionListString keepBrackets]
  "convert a string representing AND conditions into a list of strings. each string represents a condition" 
  (let [listOfStringCond (ref nil) barCount (ref 0) indexOpenBar (ref 0) indexCloseBar (ref 0)]
    (doseq [charsIter (range 0 (.length conditionListString))]      
      (if (= (str (.charAt conditionListString charsIter)) "(")
        (dosync           
          (alter barCount + 1)         
          (ref-set indexOpenBar  charsIter)           
          )
        )
      (if (= (str (.charAt conditionListString charsIter)) ")")
        (dosync 
          (alter barCount + 1)
          (ref-set indexCloseBar charsIter)             
          )
        )
      (if (= (deref barCount) 2)
        (dosync(ref-set barCount 0 )
          (if (= keepBrackets 1)            
              (dosync (ref-set listOfStringCond (conj (deref listOfStringCond) (.substring conditionListString (deref indexOpenBar) (+ 1 (deref indexCloseBar)) )))
                ) 
            (dosync (ref-set listOfStringCond (conj (deref listOfStringCond) (.substring conditionListString (+ 1 (deref indexOpenBar)) (deref indexCloseBar) ))))         
            )         
          )    
        )          
      )
    (deref listOfStringCond)
    )
  )

;Assuming conditions are from 2 kinds:
;1) Comparing between fields' value to a constant which can be a number or a symbol
;2) Comparing between two values in record by a given keys
(defn TestConditionsOnRecord [record listOfCondStr] 
  "gets a record-map and check if all conditions are satisfied" 
  (let [result (ref true)]   
    (doseq [con listOfCondStr :while(true? (deref result))]
      (let [vector (clojure.string/split con #"\s+")]
        (let [field (read-string (first vector)) oper (get operations (second vector)) value (read-string (last vector))]
          (let [val (get record field)]                       
            (if (or (number? value ) (symbol? value))
              (if-not (oper val value) (dosync (ref-set result false)))
              (let [valTwo (get record value)]
                (if-not (oper val valTwo) (dosync (ref-set result false)))))               
            val)
          field)
        vector)
      )
   (deref result) )  
  )

;gets a table and a string of conditions, returns a table with only the relvant records.
(defn filterSetByConditions [table stringOfCond]  
  "Filters a set accroding to conditions string (AND relationed), returns a new set which represents table" 
  (let [result (ref #{})]         
    (let [listOfCondStr (ParseByBarckets stringOfCond 0)]     
      (dosync (doseq [record table]
                (if (TestConditionsOnRecord record listOfCondStr)
                  (insertRecord result record))
                ))listOfCondStr)
  (deref result))
  )

;A handler to be sent to InsertOrDeleteQueryHandler when we are in DELETE mode
;syntax: DELETE Tblname WHERE (cond1) AND (cond2)...
;if subquery is empty we assume the user requests to delete all records in a table 
(defn HandleDeleteQuery [tblName subQuery]
  "Handles delete queary"
  (if-not (empty? subQuery) 
    (let [whereClauses (last (clojure.string/split subQuery #"\s+" 2))]
      (let [filteredTable (filterSetByConditions (deref (findTableIn tblName tablesNames)) whereClauses)]
        (doseq [record filteredTable]
          (deleteRecord tblName record)
          )))
    (doseq [record (deref (findTableIn tblName tablesNames))]
      (deleteRecord tblName record)
      )))

;A handler to be sent to InsertOrDeleteQueryHandler when we are in INSERT mode 
;syntax: INSERT TblName VALUES {:key1 val1 :key2 val2....}
(defn GetValuesFromQuery [tblName subQuery]
  "finds the map of keys & values representing the new record to be inserted into the table "  
    (let [values (last (clojure.string/split subQuery #"\s+" 2))]
      (let [record (read-string values)]
         (if-not (empty? record) 
           (insert tblName record)) 
         record)values)) 

;gets an insert or delete queries commands and a specific function
(defn InsertOrDeleteQueryHandler [query func]
  "Handles inserts & delete queries"  
  (let [tblName (second (clojure.string/split query #"\s+" 3))]  
     (let [subQuery (last (clojure.string/split query #"\s+" 3))] 
     (func tblName subQuery)
  subQuery)tblName))

;syntax : DROP TABLE tablename
(defn DropTableHandler [query]
  "Handles drop table query"
  (let [tblName(last (clojure.string/split query #"\s+" 3))]
    (dropTable tblName)
  tblName))

;syntax : CREATE TABLE tableName COLUMNS (:k :r :l)
(defn CreateTableHandler [query]
  "Handles create query"
  (if-not (> (count(clojure.string/split query #"\s+"))4)
    (throw (Exception. "Incorrect query"))
    )
  (let [tblName(nth (clojure.string/split query #"\s+" 5)2)]
    (let [columns (last (clojure.string/split query #"\s+" 5))]
      (createTable tblName)
      (defineColumns tblName (read-string columns))
      columns)tblName))

;util function for finding if where condition are found in string
(defn QueryContainsWhere [query]
  "Checks if query contains where clauses and returns true\false"
  (let [result (ref false)]
  (doseq [x (clojure.string/split query #"\s+")]
             (if (= x "WHERE")
               (dosync (ref-set result true))))
  (deref result)))

;syntax SELECT (field1, field2 ,field3..) FROM (table1,table2) WHERE (cond1) AND (cond2)...
(defn SelectQueryHandler [query]
  "Select Query Handler"
  (let [paramList (reverse(ParseByBarckets query 1)) table (ref #{}) result (ref #{})]
  (let [fields (first paramList)]
    (let [tables (second paramList)]
       (let [fieldsList (read-string fields)]
         (let [tablesList (read-string tables)]
            (if (< 1 (count tablesList))
                     (dosync (ref-set table (cartesianMultiply (str(first tablesList)) (str(second tablesList)))))
                     (dosync (ref-set table  (deref (findTableIn (str(first tablesList)) tablesNames))))
                     )
           (if (QueryContainsWhere query)
                 (let [whereClauses (aget (.split query "WHERE\\s+" 2) 1)]      
                   (let [filteredTable (filterSetByConditions (deref table) whereClauses)]                     
                     (dosync (ref-set result (getSelected filteredTable fieldsList)))))
                  (dosync (ref-set result (getSelected (deref table) fieldsList)))
           )))))
  (deref result)))



;Update query form: UPDATE TableName SET key1 = val1 WHERE (cond1) AND (cond2)...
(defn UpdateQueryhandler [query]
  "handles an update query"
  (let [tblName (second (clojure.string/split query #"\s+" 3))]    
    (let [tableRef (findTableIn tblName tablesNames)]      
      (let [field (read-string (nth (clojure.string/split query #"\s+" 7) 3))]          
        (let [value (read-string (nth (clojure.string/split query #"\s+" 7) 5))]              
          (let [whereClause (last (clojure.string/split query #"\s+" 8))]                     
            (let [filteredTable (filterSetByConditions (deref tableRef) whereClause)]              
              (doseq [record filteredTable] (deleteRecord tblName record))                             
               (doseq [record filteredTable]               
                 (insert tblName (assoc record field value))
                 ))))))))

(defmulti ParseQuery (fn [query] (first (clojure.string/split query #"\s+"))) )

(defmethod ParseQuery "INSERT" [query]
  (InsertOrDeleteQueryHandler query GetValuesFromQuery))

(defmethod ParseQuery "SELECT" [query]  
  (SelectQueryHandler query))

(defmethod ParseQuery "UPDATE" [query] 
  (UpdateQueryhandler query))

(defmethod ParseQuery "DELETE" [query]
  (InsertOrDeleteQueryHandler query HandleDeleteQuery))

(defmethod ParseQuery "DROP" [query]
  (DropTableHandler query))

(defmethod ParseQuery "CREATE" [query]
  (CreateTableHandler query))

(defmethod ParseQuery :default [query]
 (println "Incorrect query"))
 


