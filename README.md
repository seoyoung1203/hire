- 테이블 삭제
DROP TABLE employees; >> 연결된 hdfs에 올라간 파일 같이 삭제

- 파일 외부에 저장할거야 외부 파일 기준으로 데이터 생성해줘(데이터 있는 상황에서 데이터를 만드는 경우) 폴더 안에 파일을 여러개 
CREATE EXTERNAL TABLE employees >> 파일 삭제(DROP tables;) 해도 hdfs는 유지. 개별적 관리 

LOCATION '/input/employees'; (내가 불러오고 싶은 파일의 경로)

----------

- HAVING >> 특정 조건에 만족하는 데이터를 가지고 있나요?

- SHOW tables; >> 생성된 테이블을 보여주기 
-----------
- load data
```shell
LOAD DATA LOCAL INPATH '/home/ubuntu/damf2/data/employees'
INTO TABLE employees;
```
SELECT * FROM employees LIMIT 10

- 전체 데이터 갯수 확인
SELECT COUNT(*) FROM employees;

- 데이터 확인
SELECT * FROM employees LIMIT 10; (10개만 볼게염)

- 생일이 같은 사람 수 카운트
SELECT birth_date, COUNT(birth_date)
FROM employees
GROUP BY birth_date
LIMIT 10;

------------
