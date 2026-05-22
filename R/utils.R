# utils -------------------------------------------------------------------------

#' @noRd
`%not_in%` <- Negate(`%in%`)

#' @noRd
not_null <- Negate(is.null)

#' @noRd
not_na <- Negate(is.na)

#' @noRd
n_unique <- function(vec) vec |> unique() |> length()
