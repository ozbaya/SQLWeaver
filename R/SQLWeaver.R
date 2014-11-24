weaveSQL <- function(page, var_list, repeat_var_list=NULL) {
  for (ctr1 in 1:nrow(var_list)) {
    page<-gsub(paste("${", var_list[ctr1, "variable"], "}", sep=""), var_list[ctr1, "value"], page, fixed=TRUE)
  }

  ret <- ""
  
  if(!is.null(repeat_var_list)) {
    for(ctr1 in 1:length(page)) {
      if(regexpr("${repeat order=",page[ctr1],fixed=T) > 0) {
        for(ctr2 in 1:length(repeat_var_list)) {
          result <- regexpr(paste("${repeat order=", ctr2, "}", sep=""),page[ctr1],fixed=T)
          if(result>0) {
            result2 <- regexpr("${/repeat}",page[ctr1],fixed=T)
            temp_line<-substr(page[ctr1],result[1]+attributes(result)$match.length,result2[1]-1)

            for (ctr3 in 1:nrow(repeat_var_list[[ctr2]])) {
              for (ctr4 in 1:ncol(repeat_var_list[[ctr2]])) {
                temp_line<-gsub(paste("${", names(repeat_var_list[[ctr2]])[ctr4], "}", sep=""), repeat_var_list[[ctr2]][ctr3,ctr4], temp_line, fixed=TRUE)
              }
              ret <- c(ret,temp_line)
            }
          }
        }
      } else {
        ret <- c(ret,page[ctr1])
      }
    }
  } else {
    ret <- page
  }
  
  return_val <- ""
  for(ctr1 in 1:length(ret)) {
    if (nchar(ret[ctr1]) > 0) {
      return_val <- paste(return_val, ret[ctr1], "\n", sep="")
    }
  }
  
  return(return_val)
}


executeQuery <- function(query_str,DW,log_file=NULL) {
  temp_query_str <- strsplit(query_str, ";", fixed = TRUE)[[1]]
  #Sometimes there are multiple ;s. Get rid of them.
  temp_query_str <- temp_query_str[nchar(temp_query_str) > 10]
  
  if (length(temp_query_str) == 1) {
    ret <- sqlQuery(DW, query_str)
  } else {
    ret <- ""
    for(i in 1:length(temp_query_str)) {
      ret <- paste(ret, sqlQuery(DW, temp_query_str[i]), sep="\n\n")
    }
  }
  if(!is.null(log_file)) {
    sink(log_file)
    cat(ret)
    sink(NULL)
  }
  return(ret)
}
