\name{executeQuery}
\alias{executeQuery}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Execute a SQL script}
\description{Provided a connection, this function executes given scripts}
\usage{executeQuery(query_str, DW, log_file = NULL)}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{query_str}{
%%     ~~Describe \code{query_str} here~~
}
  \item{DW}{
%%     ~~Describe \code{DW} here~~
}
  \item{log_file}{
%%     ~~Describe \code{log_file} here~~
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
function (query_str, DW, log_file = NULL)
{
    temp_query_str <- strsplit(query_str, ";", fixed = TRUE)[[1]]
    temp_query_str <- temp_query_str[nchar(temp_query_str) >
        10]
    if (length(temp_query_str) == 1) {
        ret <- sqlQuery(DW, query_str)
    }
    else {
        ret <- ""
        for (i in 1:length(temp_query_str)) {
            ret <- paste(ret, sqlQuery(DW, temp_query_str[i]),
                sep = "\n\n")
        }
    }
    if (!is.null(log_file)) {
        sink(log_file)
        cat(ret)
        sink(NULL)
    }
    return(ret)
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
