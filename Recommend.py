from redis import StrictRedis, ConnectionPool
import time
REDIS_SETTINGS = ("0.0.0.0", 6378)
REDIS_PASSWORD = ""
REDIS_DB_INDEX = 0
REDIS_MAX_CONNECTIONS = 5

DAY_TO_SECONDS = 86400

StoredRedis = StrictRedis(
    connection_pool=ConnectionPool(host=REDIS_SETTINGS,
                                   port=REDIS_SETTINGS,
                                   password=REDIS_PASSWORD,
                                   db=REDIS_DB_INDEX,
                                   max_connections=REDIS_MAX_CONNECTIONS,
                                   decode_responses=True)
)


class Recommend(StoredRedis):
    """
    최근 접속한 유저 or 물건 등을 추천해 주는 Class
    세팅된 시간을 순차적으로 돌며 가져온다
    """
    key = "RECOMMEND:%s"

    @classmethod
    def set(cls, country, object_id):
        """
        로그인 or 검색 등에서 사용
        :param country: 국가별
        :param object_id: player_id or object_id
        :return:
        """
        score = cls.get_score()
        cls.zadd(cls.key % (country,), score, object_id)

    @classmethod
    def get(cls, country, amount):
        """
        # - 0시간 ~- 9시간 순차대로 정보를 가져온다
        :param country: 국가별
        :param amount: 100
        :return: list of object_id
        """
        score = cls.get_score()
        target_list = list()
        last_score = score
        with cls.pipeline(transaction=False) as pipe:
            for _ in [0, 1, 3, 6, 9]:
                pipe.zrangebyscore(cls.key % (country, ), last_score - _, last_score,
                                   start=0, num=amount * 2) or list()
                target = pipe.execute()

                last_score = last_score - _
                if not target:
                    continue
                # 동 시간인경우 인원이 중복 될 수도 있다, transaction 을 사용하지 않기에 set 사용하여 중복제거
                target_list = list(set(target_list + target))
                if len(target_list) > amount:
                    break

            # 첫 세팅일때 내용이 없던가 하면 추가적인 작업이 필요하다
            if not target_list:
                # DB등 에서 뭔가 가져오자, 가져왔으면 default 로 세팅겸 작업
                # target_list = SELECT something FROM DB
                for _ in target_list:
                    pipe.zadd(cls.key % (country, ), score, _)
                pipe.execute()
        # max 가 이미 넘친다면 제거
        if len(target_list) > amount:
            cls.flush_score(country, score-10)

        return target_list

    @classmethod
    def flush(cls, country, object_id):
        """
        검색 대상에서 개별 제거
        :param country: 국가별
        :param object_id: player_id or object_id
        :return:
        """
        cls.zrem(cls.key % (country,), object_id)

    @classmethod
    def flush_score(cls, country, score_min, score_max=-1):
        """
        검색 대상에서 제거 할 시간 Base score
        필요에따라 사용
        :param country: 국가별
        :param score_min: 제거 할 시간 Base score min
        :param score_max: 제거 할 시간 Base score max
        :return:
        """
        cls.zremrangebyscore(cls.key % (country,), score_min, score_max)

    @classmethod
    def get_score(cls):
        """
        나중에 조건들이 필요하면 여기다가 적용하자
        게임같은경우 level 을 붙일 수 있고
        상품같은경우 품목을 나눠서도 할 수 있다
        time 이 너무 길다면
        # 1년 31536000 (365 * 24 * 60 * 60), 1970년 기준 52년
        # 숫자를 줄일 수 있다 but 서비스 기간 고려해서 다른방법이 좋을 수도 있다
        str(int(Time.get_timestamp()) - (52 * 365 * 24 * 60 * 60)).zfill(10)

        ex)
        return f"int(time.time() / (DAY_TO_SECONDS / 24)){level}"
        """
        return int(time.time() / (DAY_TO_SECONDS / 24))  # 1시간
