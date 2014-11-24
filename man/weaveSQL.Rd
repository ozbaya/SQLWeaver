\name{weaveSQL}
\alias{weaveSQL}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Build a script}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
weaveSQL(page, var_list, repeat_var_list = NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{page}{
%%     ~~Describe \code{page} here~~
}
  \item{var_list}{
%%     ~~Describe \code{var_list} here~~
}
  \item{repeat_var_list}{
%%     ~~Describe \code{repeat_var_list} here~~
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (page, var_list, repeat_var_list = NULL)
{
    for (ctr1 in 1:nrow(var_list)) {
        page <- gsub(paste("${", var_list[ctr1, "variable"],
            "}", sep = ""), var_list[ctr1, "value"], page, fixed = TRUE)
    }
    ret <- ""
    if (!is.null(repeat_var_list)) {
        for (ctr1 in 1:length(page)) {
            if (regexpr("${repeat order=", page[ctr1], fixed = T) >
                0) {
                for (ctr2 in 1:length(repeat_var_list)) {
                  result <- regexpr(paste("${repeat order=",
                    ctr2, "}", sep = ""), page[ctr1], fixed = T)
                  if (result > 0) {
                    result2 <- regexpr("${/repeat}", page[ctr1],
                      fixed = T)
                    temp_line <- substr(page[ctr1], result[1] +
                      attributes(result)$match.length, result2[1] -
                      1)
                    for (ctr3 in 1:nrow(repeat_var_list[[ctr2]])) {
                      for (ctr4 in 1:ncol(repeat_var_list[[ctr2]])) {
                        temp_line <- gsub(paste("${", names(repeat_var_list[[ctr2]])[ctr4],
                          "}", sep = ""), repeat_var_list[[ctr2]][ctr3,
                          ctr4], temp_line, fixed = TRUE)
                      }
                      ret <- c(ret, temp_line)
                    }
                  }
                }
            }
            else {
                ret <- c(ret, page[ctr1])
            }
        }
    }
    else {
        ret <- page
    }
    return_val <- ""
    for (ctr1 in 1:length(ret)) {
        if (nchar(ret[ctr1]) > 0) {
            return_val <- paste(return_val, ret[ctr1], "\n",
                sep = "")
        }
    }
    return(return_val)
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
