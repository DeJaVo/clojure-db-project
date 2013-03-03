;Sheira Ben Haim
;Dvir Segal      


(ns tests (:use project))

(print "Defining tables... ")
(ParseQuery "CREATE TABLE students COLUMNS (:sid :first-name :last-name :year :faculty)")
(ParseQuery "CREATE TABLE courses COLUMNS (:cid :name :lecturer :c-year :passing-grade)") 
(ParseQuery "CREATE TABLE classes COLUMNS (:class-id :capacity :course-name :day)") 
(println "Done.")   

; inserting data into the tables
(print "Adding data to students...")
(ParseQuery "INSERT students VALUES {:sid 123456789 :first-name Israel :last-name Israeli :year 2 :faculty CS}")
(ParseQuery "INSERT students VALUES {:sid 234567891 :first-name Arik :last-name Berman :year 1 :faculty Math}")
(ParseQuery "INSERT students VALUES {:sid 345678912 :first-name Yehuda :last-name Levy :year 5 :faculty History}")
(ParseQuery "INSERT students VALUES {:sid 456789123 :first-name Michael :last-name Jackson :year 4 :faculty Math}")
(ParseQuery "INSERT students VALUES {:sid 567891234 :first-name Avram :last-name Grant :year 3 :faculty CS}")
(ParseQuery "INSERT students VALUES {:sid 678912345 :first-name Lady :last-name Gaga :year 5 :faculty Electricty}")
(ParseQuery "INSERT students VALUES {:sid 789123456 :first-name Bisli :last-name Gril :year 2 :faculty CS}")
(ParseQuery "INSERT students VALUES {:sid 963852741 :first-name Bill :last-name Klinton :year 1 :faculty History}")
(ParseQuery "INSERT students VALUES {:sid 546832498 :first-name Amram :last-name Mizna :year 4 :faculty Electricty}")
(ParseQuery "INSERT students VALUES {:sid 147258369 :first-name Zipi :last-name Livni :year 3 :faculty Bioligy}")
(println "Done.")

(print "Adding data to courses...")
(ParseQuery "INSERT courses VALUES {:cid 1111111 :name Clojure :lecturer Yoav :c-year 2 :passing-grade 51}")
(ParseQuery "INSERT courses VALUES {:cid 2222222 :name DataBases :lecturer Dany :c-year 1 :passing-grade 60}")
(ParseQuery "INSERT courses VALUES {:cid 3333333 :name Physics :lecturer Moshe :c-year 1 :passing-grade 75}")
(ParseQuery "INSERT courses VALUES {:cid 4444444 :name WorldWarHistory :lecturer Guy :c-year 2 :passing-grade 51}")
(ParseQuery "INSERT courses VALUES {:cid 5555555 :name Genetics :lecturer Shuli :c-year 3 :passing-grade 45}")
(ParseQuery "INSERT courses VALUES {:cid 7894561 :name OOP :lecturer Gadi :c-year 4 :passing-grade 70}")
(ParseQuery "INSERT courses VALUES {:cid 9638527 :name ElectricSignals :lecturer Avi :c-year 2 :passing-grade 51}")
(ParseQuery "INSERT courses VALUES {:cid 4562597 :name IsraelHistory :lecturer Mark :c-year 2 :passing-grade 55}")
(ParseQuery "INSERT courses VALUES {:cid 4455667 :name Probability :lecturer Ronen :c-year 1 :passing-grade 80}")
(ParseQuery "INSERT courses VALUES {:cid 3366998 :name Calculus :lecturer Or :c-year 1 :passing-grade 51}")
(println "Done.")

(print "Adding data to classes...")
(ParseQuery "INSERT classes VALUES {:class-id 1 :capacity 50 :course-name Calculus :day SUN}")
(ParseQuery "INSERT classes VALUES {:class-id 2 :capacity 100 :course-name ElectricSignals :day MON}")
(ParseQuery "INSERT classes VALUES {:class-id 3 :capacity 75 :course-name OOP :day TUE}")
(ParseQuery "INSERT classes VALUES {:class-id 4 :capacity 20 :course-name WorldWarHistory :day WEN}")
(ParseQuery "INSERT classes VALUES {:class-id 5 :capacity 45 :course-name Clojure :day SUN}")
(println "Done.")


; Running 2 UPDATE queries and printing results
(println "students initial state:")
(println (ParseQuery "SELECT (:sid :first-name :last-name :year :faculty) FROM (students)"))
(println "")
(ParseQuery "UPDATE students SET :last-name = Tofel WHERE (:year = 2)")
(println "students state after 1st UPDATE query (2 records were affected):")
(println (ParseQuery "SELECT (:sid :first-name :last-name :year :faculty) FROM (students)"))
(println "")
(ParseQuery "UPDATE students SET :year = 5 WHERE (:faculty = CS) AND (:year = 2)")
(println "students state after 2nd UPDATE query (2 record was affected):")
(println (ParseQuery "SELECT (:sid :first-name :last-name :year :faculty) FROM (students)"))

(println "")
(println "")
; Running 2 DELETE queries and printing results
(println "courses initial state:")
(println (ParseQuery "SELECT (:cid :name :lecturer :c-year :passing-grade) FROM (courses)"))
(println "")
(ParseQuery "DELETE courses WHERE (:lecturer = Dany) AND (:c-year = 1) AND (:passing-grade = 60)")
(println "courses state after 1st DELETE query (1 records were deleted):")
(println (ParseQuery "SELECT (:cid :name :lecturer :c-year :passing-grade) FROM (courses)"))
(println "")
(ParseQuery "DELETE courses WHERE (:c-year > 1) AND (:passing-grade = 51)")
(println "courses state after 2nd DELETE query (3 record was deleted):")
(println (ParseQuery "SELECT (:cid :name :lecturer :c-year :passing-grade) FROM (courses)"))
(println "")

; Running 3 SELECT queries and printing results
(println "")
(println "Showing Courses been taught at sunday with their course's Id ,name and day:")
(println (ParseQuery "SELECT (:cid :name :day) FROM (courses classes) WHERE (:course-name = :name) AND (:day = SUN)"))
(println "")
(println "Showing students who study at CS faculty and take courses of 2nd year which their passing grade is less than 70:")
(println (ParseQuery "SELECT (:first-name :last-name) FROM (students courses) WHERE (:c-year >= 2) AND (:c-year = :year) AND (:faculty = CS) AND (:passing-grade < 70)"))
(println "")
(println "Showing Classes which their class id is equal or higher than 3, their capacity is lower than 75 and take place at sundays:")
(println (ParseQuery "SELECT (:class-id :capacity :day) FROM (classes) WHERE (:class-id >= 3) AND (:capacity < 75) AND (:day = SUN) )"))

;Running 3 DROP queries and printing empty database:
(println "")
(println "Removing DataBase")
(ParseQuery "DROP TABLE students")
(ParseQuery "DROP TABLE courses")
(ParseQuery "DROP TABLE classes")
