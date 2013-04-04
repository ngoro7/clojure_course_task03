(ns clojure-course-task03.core
  (:require [clojure.set])
  (:require [clojure.string :as s])
  )

(defn join* [table-name conds]
  (let [op (first conds)
        f1 (name (nth conds 1))
        f2 (name (nth conds 2))]
    (str table-name " ON " f1 " " op " " f2)))

(defn where* [data]
  (let [ks (keys data)
        res (reduce str (doall (map #(let [src (get data %)
                                           v (if (string? src)
                                               (str "'" src "'")
                                               src)]
                                       (str (name %) " = " v ",")) ks)))]
    (reduce str (butlast res))))

(defn order* [column ord]
  (str (name column)
       (if-not (nil? ord) (str " " (name ord)))))

(defn limit* [v] v)

(defn offset* [v] v)

(defn -fields** [data]
  (reduce str (butlast (reduce str (doall (map #(str (name %) ",") data))))))

(defn fields* [flds allowed]
  (let [v1 (apply hash-set flds)
        v2 (apply hash-set allowed)
        v (clojure.set/intersection v1 v2)]
    (cond
     (and (= (first flds) :all) (= (first allowed) :all)) "*"
     (and (= (first flds) :all) (not= (first allowed) :all)) (-fields** allowed)
     (= :all (first allowed)) (-fields** flds)
     :else (-fields** (filter v flds)))))

(defn select* [table-name {:keys [fields where join order limit offset]}]
  (-> (str "SELECT " fields " FROM " table-name " ")
      (str (if-not (nil? where) (str " WHERE " where)))
      (str (if-not (nil? join) (str " JOIN " join)))
      (str (if-not (nil? order) (str " ORDER BY " order)))
      (str (if-not (nil? limit) (str " LIMIT " limit)))
      (str (if-not (nil? offset) (str " OFFSET " offset)))))


(defmacro select [table-name & data]
  (let [;; Var containing allowed fields
        fields-var# (symbol (str table-name "-fields-var"))

        ;; The function takes one of the definitions like (where ...) or (join ...)
        ;; and returns a map item [:where (where* ...)] or [:join (join* ...)].
        transf (fn [elem]
                 (let [v (first elem)
                       v2 (second elem)
                       v3 (if (> (count elem) 2) (nth elem 2) nil)
                       val (case v
                               fields (list 'fields* (vec (next elem)) fields-var#)
                               offset (list 'offset* v2)
                               limit (list 'limit* v2)
                               order (list 'order* v2 v3)
                               join (list 'join* (list 'quote v2) (list 'quote v3))
                               where (list 'where* v2))]
                   [(keyword v) val]))

        ;; Takes a list of definitions like '((where ...) (join ...) ...) and returns
        ;; a vector [[:where (where* ...)] [:join (join* ...)] ...].
        env* (loop [d data
                    v (first d)
                    res []]
               (if (empty? d)
                 res
                 (recur (next d) (second d) (conj res (transf v)))))

        ;; Accepts vector [[:where (where* ...)] [:join (join* ...)] ...],
        ;; returns map {:where (where* ...), :join (join* ...), ...}
        env# (apply hash-map (apply concat env*))]
    
    `(select* ~(str table-name)  ~env#)))


;; Examples:
;; -------------------------------------

(let [proposal-fields-var [:person, :phone, :address, :price]]
  (select proposal
          (fields :person, :phone, :id)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(let [proposal-fields-var [:person, :phone, :address, :price]]
  (select proposal
          (fields :all)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(let [proposal-fields-var [:all]]
  (select proposal
          (fields :all)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))


(comment
  ;; Описание и примеры использования DSL
  ;; ------------------------------------
  ;; Предметная область -- разграничение прав доступа на таблицы в реелтерском агенстве
  ;;
  ;; Работают три типа сотрудников: директор (имеет доступ ко всему), операторы ПК (принимают заказы, отвечают на тел. звонки,
  ;; передают агенту инфу о клиентах), агенты (люди, которые лично встречаются с клиентами).
  ;;
  ;; Таблицы:
  ;; proposal -> [id, person, phone, address, region, comments, price]
  ;; clients -> [id, person, phone, region, comments, price_from, price_to]
  ;; agents -> [proposal_id, agent, done]

  ;; Определяем группы пользователей и
  ;; их права на таблицы и колонки
  (group Agent
         proposal -> [person, phone, address, price]
         agents -> [clients_id, proposal_id, agent])

  ;; Предыдущий макрос создает эти функции
  (select-agent-proposal) ;; select person, phone, address, price from proposal;
  (select-agent-agents)  ;; select clients_id, proposal_id, agent from agents;




  (group Operator
         proposal -> [:all]
         clients -> [:all])

  ;; Предыдущий макрос создает эти функции
  (select-operator-proposal) ;; select * proposal;
  (select-operator-clients)  ;; select * from clients;



  (group Director
         proposal -> [:all]
         clients -> [:all]
         agents -> [:all])

  ;; Предыдущий макрос создает эти функции
  (select-director-proposal) ;; select * proposal;
  (select-director-clients)  ;; select * from clients;
  (select-director-agents)  ;; select * from agents;
  

  ;; Определяем пользователей и их группы

  (user Ivanov
        (belongs-to Agent))

  (user Sidorov
        (belongs-to Agent))

  (user Petrov
        (belongs-to Operator))

  (user Directorov
        (belongs-to Operator,
                    Agent,
                    Director))


  ;; Оператор select использует внутри себя переменную <table-name>-fields-var.
  ;; Для указанного юзера макрос with-user должен определять переменную <table-name>-fields-var
  ;; для каждой таблицы, которая должна содержать список допустимых полей этой таблицы
  ;; для этого пользователя.

  ;; Агенту можно видеть свои "предложения"
  (with-user Ivanov
    (select proposal
            (fields :person, :phone, :address, :price)
            (join agents (= agents.proposal_id proposal.id))))

  ;; Агенту не доступны клиенты
  (with-user Ivanov
    (select clients
            (fields :all)))  ;; Empty set

  ;; Директор может видеть состояние задач агентов
  (with-user Directorov
    (select agents
            (fields :done)
            (where {:agent "Ivanov"})
            (order :done :ASC)))
  
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; group
;; user
;; with-user
;;  macros implementation
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; fields sanity:
;; ['person, 'phone, 'address, 'price] -> ['person, 'phone, 'address, 'price]
;; ['person, :all, 'phone, 'address, 'price] -> [:all]
(defn prep-table-fields [flds]
	(if (some #{:all} flds)
	  [:all]
	  (vec flds)))

;; ['person 'phone 'address 'price]  -> person,phone,address,price
;; ['person :all  'phone 'address 'price] -> *
(defn select-fields-str [flds]
	(if (= :all (first flds))
	  "*"
	  (s/join "," flds)))


(defn to-keywords-vec [xs]
	(vec (map #(if (keyword? %) % (keyword %)) xs)))


;; Merge 2 vectors get rid of duplicated fields
;; If :all present then returns [:all]
(defn merge-fields [flds1 flds2]
	(let [set1 (set flds1)
	      set2 (set flds2)
	      uset (clojure.set/union set1 set2)]
	  (prep-table-fields (vec uset))))

;; Merge 2 vectors get rid of duplicated fields
;; If :all present then returns [:all]
(defn merge-fields [flds1 flds2]
	(let [set1 (set flds1)
	      set2 (set flds2)
	      uset (clojure.set/union set1 set2)]
	  (prep-table-fields (vec uset))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Macro group
;; Syntax:
;; (group Agent
;;      proposal -> [person, phone, address, price]
;;      agents -> [clients_id, proposal_id, agent])  
;; 
;; Generates:
;; 1) (defn select-agent-agents ....
;; which produce sql select strings:
;;   "SELECT clients_id,proposal_id,agent FROM agents "
;; (defn select-agent-agents ....
;; which produce sql select strings:
;;   "SELECT person,phone,address,price FROM proposal "
;; 2) List of group tables:
;;  (def group-agent-tables [:agents, :proposal])
;; 3) defs:
;;  (def group-agent-agents-fields [:clients_id :proposal_id :agent])
;;  (def group-agent-proposal-fields [:person :phone :address :price])

(defmacro group [name & body]
  ;; Argumets check
  ;; body should be empty or list of triplets
  {:pre [(or (nil? body )
         (= (mod (count body) 3) 0))]}
  ;;
   (let [table-elems (partition 3 body)
         name (s/lower-case name)]
    `(do
       ~@(map
         (fn [[table-name arrow fields]]
           (if-not (= arrow '->)
             (throw (IllegalArgumentException. "Invalid group expression: absent '->'")))
           (let [table-name (->> table-name str s/lower-case)
                 select-fn-name (str "select-" name "-" table-name)
                 fields (prep-table-fields fields)]
           `(do
             (defn ~(symbol select-fn-name) []
             (format "SELECT %s FROM %s " ~(select-fields-str fields) ~table-name))
             (def ~(symbol (format "group-%s-%s-fields" name table-name)) ~(to-keywords-vec fields)))
             )) table-elems )

        (def ~(symbol (format "group-%s-tables" name))
          ~(->>
            table-elems
            (map first)
            (map keyword)
            vec))
        )))


;; (group-tables "agent")
;; ->
;; (["proposal" "agent"] ["agents" "agent"])
(defn group-tables [group-name]
  (let [grdef (str "group-" group-name "-tables")
        tables (deref (resolve (symbol grdef)))]
    (for [t tables] [(name t) group-name])))

;; Table and their group list
;; Get groups list
;; e.g.  ["agent" "operator"]
;; and genarate list like (("table1" ("group1" "group2")  ("table2" ("group3"))
;; For example: (("proposal" ("agent" "operator")) ("agents" ("agent")) ("clients" ("operator")))
(defn table-groups [groups]
(->> groups
  (map group-tables)
  (reduce concat)
  (group-by first)
  (map #(list (first %) (map second (second %))))))

;; Get group and table names
;; and return table fields for this group
;; Example:
;; (group-table-fields "agent" "agents")
;; may result
;; [:clients_id :proposal_id :agent]
(defn group-table-fields [group table]
  (let [ident (str "group-" group "-" table "-fields")]
    (deref (resolve (symbol ident)))))

;; Merge 2 vectors get rid of duplicated fields
;; If :all present then returns [:all]
(defn merge-fields [flds1 flds2]
  (let [set1 (set flds1)
        set2 (set flds2)
        uset (clojure.set/union set1 set2)]
    (prep-table-fields (vec uset))))

;; Store map like this:
;; {"User1" #{"tbl1" "tbl2"}, "User2" #{"tbl1" "tbl3" "tbl5"}}
(def ^:dynamic *user-tables-vars* (atom {}))

;; Register user's table name
(defn register-user-table-name [user-name table-name]
  (swap! *user-tables-vars* (partial merge-with clojure.set/union {user-name (hash-set table-name)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Based on group permissions on tables fields
;; generates defs like:
;; (do (def Ivanov-proposal-fields-var [:all]) (def Ivanov-agents-fields-var [:clients_id :proposal_id :agent]) )
;; Also regiter table name in *user-tables-vars* atom
(defmacro user [user-name & groups]
	(assert (not (nil? groups)) "Syntax error: Groups list not found")
  (let [body (first groups)
        belongs-to (first body)]
    (assert (= (name belongs-to) "belongs-to") "Syntax error: 'belongs-to' not found")
    (let [groups (rest body)]
      (assert (not (empty? groups)) "User should belongs to at least one group")
      (let [tg (table-groups (->> groups (map name) (map s/lower-case)))]

        `(do
          ~@(map (fn [[table groups]]
	           (let [all-fields (map #(group-table-fields % table) groups)
		               fields (reduce merge-fields all-fields)]
		          `(do
		            (register-user-table-name ~(name user-name) ~table)
	              (def ~(symbol (str user-name "-" table "-fields-var")) ~fields))
	           )) tg ))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro with-user [username & body]
(if-not (nil? body)
  (let [username (name username)
        tables (get @*user-tables-vars* username)
        bnds (vec (mapcat #(vector (symbol (str % "-fields-var")) (symbol (str username "-" % "-fields-var"))) tables))]
	`(let ~bnds ~@body)
   )))
