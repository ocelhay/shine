# plotly_theme -----------------------------------------------------------------

#' Dashboard plotly theme
#'
#' Applies consistent hover styling and removes the plotly modebar except for
#' the download-to-image button.
#'
#' @param pp A plotly object.
#' @param ... Additional arguments passed to `plotly::layout()`.
#' @return A plotly object.
#' @export
dashboard_plotly_theme <- function(pp, ...) {
  pp |>
    plotly::layout(
      hoverlabel = list(
        font    = list(family = "Inter", size = 13),
        bgcolor = "white"
      ),
      ...
    ) |>
    plotly::config(
      displaylogo    = FALSE,
      modeBarButtons = list(list("toImage"))
    )
}
