# ECommerceRecommendSystem
test: share spark project from idea to git

Structure:
ECommerceRecommendSystem
  -- businessServer
  -- recommender
       -- DataLoader
       -- StatisticsRecommender
       -- OfflineReommender
       -- KafkaStreaming
       -- OnlineRecommender
       -- ContentRecommender
       -- ItemCFRecommender


TODO:
1) 打jar包提交到集群环境测试部署
2) 数据库加密, 主要是redis加密
3) 代码结构优化，configure类统一管理 