remotes::install_github("rstudio/chromote")
library(chromote)
# code adapated from https://github.com/rundel/hayalbaz
# MIT license: https://github.com/rundel/hayalbaz/commit/f68be8df82fb98ee0141c1fee799e1d9a3da947f#diff-9cc358405149db607ff830a16f0b4b21f7366e3c99ec00d52800acebe21b231cR11

# chromote uses https://chromedevtools.github.io/devtools-protocol/
b <- NA
default_timeout <- 5 # seconds
close_session <- function() {
  tryCatch({
    if(b$.__enclos_env__$private$is_active_) b$close()},
    error=function(error_message) {
      message("Could not close session")
    })
}

start_session <- function() {
  close_session()
  message("Starting new chrome session...")
  b <<- ChromoteSession$new()
}

view_session <- function() {
  b$view()
}

navigate <- function(url) {
  b$Page$navigate(url)
}

get_document <- function() {
  b$DOM$getDocument()
}

node_id <- function(css_selector) {
  x <- get_document()
  b$DOM$querySelector(x$root$nodeId,css_selector)$nodeId
}

eval_js <- function(exp) {
  b$Runtime$evaluate(exp)
}

wait_exists <- function(selector,timeout=0) {
  el <- eval_js(paste0("document.querySelector('", selector ,"')"))
  if (timeout > default_timeout) {
    message("Default timeout of ",default_timeout, "seconds exceeded")
    return()
  }
  if (el$result$subtype == "null") {
    Sys.sleep(1)
    wait_exists(selector,timeout + 1) } else {
      return()
    }
}

click <- function(selector,scroll=TRUE) {

  wait_exists(selector)
  el <- eval_js(paste0("document.querySelector('", selector ,"')"))
  
  if (el$result$subtype == "null") {
    message("Unable to locate el with selector: ",selector)
    message("el ")
  } else {
    if (scroll) {
      # Not currently supported by Chromote
      #b$DOM$scrollIntoViewIfNeeded(id)
      
      # Based on https://github.com/cyrus-and/chrome-remote-interface/issues/180
      eval_js(paste0("document.querySelector('", selector ,"').scrollIntoView()"))
    }
    eval_js(paste0("document.querySelector('", selector ,"').click()"))
  }
}

clickAll <- function(selector){
  wait_exists(selector)
  selector = paste0("els = document.querySelectorAll('", selector ,"')")
  exp = paste0(selector,"; els.forEach( el => el.click())")
  eval_js(exp)
}

# xpath_selector <- function(xpath) {
#   expression <- paste0('results = document.evaluate("',xpath,'",document,null,XPathResult.ANY_TYPE, null);
#                        var returned_results = []; while(results) {returned_results.push(results); results.iterateNext();}')
# 
#   #expression
#   b$Runtime$evaluate(expression)
# }


doc_outer_html <- function() {
  b$Runtime$evaluate(
    paste0("document.documentElement.outerHTML")
  )$result$value
}
