-- example_schema
\timing off
\pset null ''
\x off
\pset pager
\set ON_ERROR_STOP on
\set QUIET 1


drop database if exists transactions ;
create database transactions;


drop database if exists metricstore ;
create database metricstore;

begin;
-- drop schema if exists public cascade;
create schema if not exists public;
set search_path to public;

\c transactions

create table if not exists tenants (
  tenant_id integer primary key generated by default as identity,
  name text
);

create table if not exists vendors (
  tenant_id integer not null references tenants,
  vendor_id integer primary key generated by default as identity,
  name text,
  credit_limit money,
  _updated_at timestamp default now()
);

create table if not exists orders (
  tenant_id integer not null,
  order_id integer primary key generated by default as identity,
  vendor_id integer references vendors,
  title text,
  budgeted_amount money,
  _updated_at timestamp default now()
);

create table if not exists orders_status (
  record_id integer primary key generated by default as identity,
  order_id integer,
  status text,
  _updated_at timestamp default now()
);

create table if not exists line_items (
  tenant_id integer not null,
  line_item_id integer primary key generated by default as identity,
  order_id integer references orders,
  title text,
  cost money,
  _updated_at timestamp default now()
);
commit;

begin;

insert into tenants (tenant_id, name) values
(1, 'A'),
(2, 'B')
on conflict do nothing;

insert into vendors (tenant_id,vendor_id, name, credit_limit) values
(1, 1, 'Acme', 10230),
(1, 2, 'National', 456.12)  -- this vendor will have no orders
,(1,3, 'Empty', 101.01)
on conflict do nothing;

insert into orders (tenant_id, order_id, vendor_id, title, budgeted_amount) values
  (1,1,1,'$100 order w/3li', 100.00)
, (1,2,1,'$200 order w/1li', 200.00)
, (1,3,1,'$302 order w/0li', 302.00)
, (1,4,3,'$404 order w/0li', 404.00)
on conflict do nothing;

insert into line_items (tenant_id, line_item_id, order_id, title, cost) values
(1,1,1,'materials', 20.00),
(1,2,1,'lumber', 340.00),
(1,3,1,'labor', 100.00),
(1,4,2,'labor', 5000.00)
on conflict do nothing;

-- SCD type 2
insert into orders_status(record_id, order_id, status) values
  (1, 1, 'PAYED')
, (2, 1, 'FULFILLED')
, (3, 2, 'CANCELLED')
, (4, 3, 'FULFILLED')
, (5, 4, 'FULFILLED')
, (6, 5, 'INVALID')
on conflict do nothing;

commit;

\d