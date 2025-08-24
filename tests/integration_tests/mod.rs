// Aggregates per-module test suites under module_tests/*
mod api;
mod cache;
mod cli;
mod collection;
mod crypto;
mod database;
mod document;
mod export;
mod import;
mod query;
mod recovery;
mod utils;
mod _support;

#[path = "mod_lib.rs"]
mod lib_tests;
