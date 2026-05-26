# utils -------------------------------------------------------------------------

#' @export
`%not_in%` <- Negate(`%in%`)

#' @export
not_null <- Negate(is.null)

#' @export
not_na <- Negate(is.na)

#' @export
n_unique <- function(vec) vec |> unique() |> length()
