require(RMySQL)
require(plyr)
require(hash)

where_sql <- "and xk_lctr.date=curdate()"

con <- dbConnect(dbDriver("MySQL"), dbname="guang", host="192.168.32.10", user="guang", password="guang")
dbGetQuery(con, "set names 'utf8'")

data = dbGetQuery(con, paste("select * from xk_lctr where 1", where_sql))
datasort <- data[order(-data$lctr),]
datasort$ctrdist <- (datasort$lctr - ave(datasort$lctr))/var(datasort$lctr)
datasort$ctrdist2 <- datasort$ctrdist/max(abs(datasort$ctrdist))
item2ctrdist <- hash(datasort$item_id, datasort$ctrdist2)

users <- dbGetQuery(con, paste("select distinct uid, uname from xk_lctr, voterecord, users where xk_lctr.task_id = voterecord.taskid and users.id=voterecord.uid", where_sql))

get_sum_by_user <- function (x) {
  votes <- dbGetQuery(con, paste(sprintf("select xk_lctr.item_id, voterecord.score from voterecord,xk_lctr where xk_lctr.task_id = voterecord.taskid and uid=%s %s", x, where_sql)))
  votes$ctrdist <- sapply(votes$item_id, function(y){item2ctrdist[[as.character(y)]]})
  p_votes <- votes[votes$score>0,]
  n_votes <- votes[votes$score<0,]
  #ifelse(c(nrow(p_votes)>0, TRUE, TRUE), c(sum(p_votes$ctrdist), nrow(p_votes), nrow(n_votes)), c(-1000, nrow(p_votes), nrow(n_votes)))
  ifelse(c(nrow(p_votes)>0, TRUE, TRUE), c(as.integer(sum(p_votes$ctrdist)), nrow(p_votes), nrow(n_votes)), c(-1000, nrow(p_votes), nrow(n_votes)))
}

#users$score <- sapply(users$uid, function(x) {get_sum_by_user(x)})
userstats <- cbind(users, t(as.data.frame(lapply(users$uid, function(x) {get_sum_by_user(x)}))))
names(userstats) <- c("uid", "uname", "score", "pos", "neg")
write.csv(userstats, file="userstats.csv")

