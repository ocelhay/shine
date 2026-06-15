# utils -------------------------------------------------------------------------

#' Not-in operator
#' @param x Vector of values to test.
#' @param table Vector of values to test against.
#' @return Logical vector.
#' @export
`%not_in%` <- function(x, table) !(x %in% table)

#' Not-null predicate
#' @param x Object to test.
#' @return Logical scalar.
#' @export
not_null <- function(x) !is.null(x)

#' Not-NA predicate
#' @param x Object to test.
#' @return Logical vector.
#' @export
not_na <- function(x) !is.na(x)

#' Count unique values
#' @param vec A vector.
#' @return Integer scalar.
#' @export
n_unique <- function(vec) vec |> unique() |> length()
