setwd("E:/Projects/SQLWeaver")
getwd()

page <- readLines("data/test.sql")
var_list <- read.table("data/values.txt",header=T,sep="\t")
rep_var_list1 <- read.table("data/repeat_vals1.txt",header=T,sep="\t")
rep_var_list2 <- read.table("data/repeat_vals2.txt",header=T,sep="\t")
rep_var_list <- list(rep_var_list1,rep_var_list2)

sink("data/test.out.sql")
cat(weaveSQL(page, var_list, rep_var_list))
sink(NULL)
