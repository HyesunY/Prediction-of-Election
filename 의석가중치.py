from sympy import symbols, Eq, solve

# 새로운 문제에 대한 변수 및 가중치 정의
pos_weight, neg_weight = symbols('pos_weight neg_weight')
a21_pos, a21_neg = 17.406, 82.594
b21_pos, b21_neg = 70.94, 29.06
a22_pos, a22_neg = 61.551, 38.449
b22_pos, b22_neg = 66.519, 33.481
total_seats = 300  # 전체 의석수 업데이트
a21_seats_percent = 34.33
b21_seats_percent = 60
c_seats = 6

# 21대 정당의 의석 계산 (새로운 비율로 업데이트)
equation1_new = Eq(a21_pos * pos_weight + a21_neg * neg_weight, a21_seats_percent * total_seats / 100)
equation2_new = Eq(b21_pos * pos_weight + b21_neg * neg_weight, b21_seats_percent * total_seats / 100)

# 새로운 가중치 계산
new_weights = solve((equation1_new, equation2_new), (pos_weight, neg_weight))

# 22대 정당의 의석 계산 (새로운 가중치 사용)
a22_seats_new = a22_pos * new_weights[pos_weight] + a22_neg * new_weights[neg_weight]
b22_seats_new = b22_pos * new_weights[pos_weight] + b22_neg * new_weights[neg_weight]

# c 정당 의석 수 (23.6석으로 업데이트)
c22_seats = 23.6

a21_seats_new = a21_pos * new_weights[pos_weight] + a21_neg * new_weights[neg_weight]
b21_seats_new = b21_pos * new_weights[pos_weight] + b21_neg * new_weights[neg_weight]

print(a21_seats_new)
print(b22_seats_new)
# 원래 21a 정당의 의석 수: 84석
# 원래 21b 정당의 의석 수: 163석
print('-----------')
print(a22_seats_new)
print(b22_seats_new)
print(new_weights)

#22a 정당의 의석 수: 약 149.17석
#22b 정당의 의석 수: 약 156.51석

# #pyspark 문법
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf
# from pyspark.sql.types import FloatType
#
# # Spark 세션 초기화
# spark = SparkSession.builder.appName("election_analysis").getOrCreate()
#
# # 데이터 프레임 생성
# data = [
#     ("21a", 17.406, 82.594),
#     ("21b", 70.94, 29.06),
#     # 여기에 추가 데이터 입력
# ]
# columns = ["party", "positive", "negative"]
# df = spark.createDataFrame(data, columns)
#
# # 가중치를 계산하는 함수 정의
# def calculate_weighted_seats(positive, negative, pos_weight, neg_weight, total_seats, percent):
#     return (positive * pos_weight + negative * neg_weight) * total_seats * percent / 100
#
# # UDF (User Defined Function) 등록
# calculate_seats_udf = udf(calculate_weighted_seats, FloatType())
#
# # 계산에 사용할 가중치 설정
# pos_weight = 2.05937168169761
# neg_weight = 0.582979108753316
# total_seats = 300
# a21_percent = 34.33
# b21_percent = 60
#
# # 데이터 프레임에 UDF 적용
# df = df.withColumn("weighted_seats", calculate_seats_udf(df["positive"], df["negative"], lit(pos_weight), lit(neg_weight), lit(total_seats), lit(a21_percent)))
#
# # 결과 출력
# df.show()
