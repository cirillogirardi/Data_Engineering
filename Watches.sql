CREATE schema watches_schema;
set schema watches_schema;


drop table if exists watches_schema.watch cascade;
drop table if exists watches_schema.brand_information cascade;
drop table if exists watches_schema.social_information cascade;
drop table if exists watches_schema.company_description cascade;
drop table if exists watches_schema.financial_information cascade;
drop table if exists watches_schema.board cascade;
drop table if exists watches_schema.characteristics cascade;
drop table if exists watches_schema.prices cascade;
drop table if exists watches_schema.seller cascade;



CREATE TABLE watches_schema.watch  (
  watch_reference varchar(255) PRIMARY KEY,
  watch_code int,
  brand varchar(255),
  watch_collection varchar(255)
);

CREATE TABLE watches_schema.brand_information  (
  brand varchar(255) PRIMARY KEY,
  holding_company varchar(255),
  address varchar(255),
  industry_type varchar(255),
  industry varchar(255)
);

CREATE TABLE watches_schema.social_information  (
  brand varchar(255) PRIMARY KEY,
  telephone_number varchar(255),
  company_website varchar(255),
  facebook varchar(255),
  twitter varchar(255),
  instagram varchar(255),
  weibo varchar(255),
  google varchar(255),
  line varchar(255),
  wechat varchar(255),
  youtube varchar(255)
);

CREATE TABLE watches_schema.company_description  (
  brand varchar(255) PRIMARY KEY,
  company_description varchar(255),
  founded varchar(255),
  founders varchar(255)
);

CREATE TABLE watches_schema.financial_information (
  holding_company varchar(255) PRIMARY KEY,
  employees varchar(255),
  revenue varchar(255),
  net_income_growth varchar(255),
  enviornmental_social_governance_ranking varchar(255),
  subsidiaries varchar(255)
);

CREATE TABLE watches_schema.board (
  holding_company varchar(255) PRIMARY KEY,
  managing_director varchar(255),
  board_chairman varchar(255),
  director varchar(255)
);

CREATE TABLE watches_schema.characteristics (
  watch_reference varchar(255) PRIMARY KEY,
  case_diameter varchar(255),
  watch_movement varchar(255),
  watch_dial varchar(255),
  watch_strap varchar(255),
  watch_markers varchar(255),
  watch_recipient varchar(255),
  watch_guarantee varchar(255)
);

CREATE TABLE watches_schema.prices (
  watch_reference varchar(255) PRIMARY KEY,
  production_year varchar(255),
  condition varchar(255),
  retail_price varchar(255),
  aftermarket_prize varchar(255),
  box_and_papers varchar(255),
  seller_id int
);

CREATE TABLE watches_schema.seller (
  seller_id int PRIMARY KEY,
  seller_location varchar(255),
  seller_rating varchar(255)
);

ALTER TABLE watch ADD FOREIGN KEY (watch_reference) REFERENCES prices (watch_reference);

ALTER TABLE watch ADD FOREIGN KEY (brand) REFERENCES brand_information (brand);

ALTER TABLE brand_information ADD FOREIGN KEY (holding_company) REFERENCES financial_information (holding_company);

ALTER TABLE brand_information ADD FOREIGN KEY (brand) REFERENCES company_description (brand);

ALTER TABLE characteristics ADD FOREIGN KEY (watch_reference) REFERENCES prices (watch_reference);

ALTER TABLE seller ADD FOREIGN KEY (seller_id) REFERENCES prices (seller_id);

ALTER TABLE board ADD FOREIGN KEY (holding_company) REFERENCES financial_information (holding_company);

ALTER TABLE company_description ADD FOREIGN KEY (brand) REFERENCES social_information (brand);
