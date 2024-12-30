/* ****************************************************************************************
  PURPOSE  : Fact Table Preparation for all purposes 
  DATE     : 27 Jul 2016
  REMARKS  : N/A
*******************************************************************************************
Version By        	Date     Description
------------------------------------------------------------------------------------------
1.0     Philip Yam  20160727 Program Framework Creation
*******************************************************************************************/

/* Parameters and Properties Definition */
option mprint ERRORS=max;
option compress=binary;
option obs=max;
option nocenter;

/* **************************************************************************************** */
/* Paths Definition 																		*/
/* **************************************************************************************** */
%let dir_tmp		= /sastmp;
%let dir_root		= /NXX/NXX_Basel;
%let dir_base		= &dir_root./SAS_ETL;
%let dir_bbr		= &dir_root./DataSource/BBR;
%let dir_bpe		= &dir_root./DataSource/BPE;
%let dir_car		= &dir_root./DataSource/CAR;
%let dir_ddm		= &dir_root./DataSource/DDM/RM1DDMON;
%let dir_rmp		= &dir_root./DataSource/RMP;
%let dir_ybc		= &dir_root./DataSource/YBC;
%let dir_ybd		= &dir_root./DataSource/YBD;
%let dir_sam		= &dir_root./DataSource/SAM/ori_weekly;
%let dir_s_p		= &dir_root./DataSource/S_P;

%let dir_parm		= &dir_base./00_Parameter;
%let dir_pgm		= &dir_base./01_Program;
%let dir_lib		= &dir_base./02_Library;
%let dir_sraw		= &dir_base./11_SRaw;
%let dir_stg		= &dir_base./12_Staging;				*Keep the staging tables for checking purposes, housekeeping and archive from time to time are required;
%let dir_fact		= &dir_base./13_FactTable;				*Create fact table with time dimension;
%let dir_mart		= &dir_base./14_DataMart;				*Project spectific;
%let dir_rptsc		= &dir_base./91_SenseReport;
%let dir_rptdq		= &dir_base./92_DQReport;
%let dir_rptpf		= &dir_base./93_PerformReport;
%let dir_log		= &dir_base./99_Log;

filename lib		"&dir_lib.";
%include			lib(Datasets.sas);
%include			lib(Macro.sas);

libname raw_oth		"&dir_sraw./oth";
libname raw_bbr		"&dir_sraw./bbr";
libname raw_bpe		"&dir_sraw./bpe";
libname raw_car		"&dir_sraw./car";
libname raw_ddm		"&dir_sraw./ddm";
libname raw_rmp		"&dir_sraw./rmp";
libname raw_ybc		"&dir_sraw./ybc";
libname raw_ybd		"&dir_sraw./ybd";
libname raw_sam		"&dir_sraw./sam";
libname raw_s_p		"&dir_sraw./s_p";

libname stg_oth		"&dir_stg./oth";
libname stg_bbr		"&dir_stg./bbr";
libname stg_bpe		"&dir_stg./bpe";
libname stg_car		"&dir_stg./car";
libname stg_ddm		"&dir_stg./ddm";
libname stg_rmp		"&dir_stg./rmp";
libname stg_ybc		"&dir_stg./ybc";
libname stg_ybd		"&dir_stg./ybd";
libname stg_sam		"&dir_stg./sam";
libname stg_s_p		"&dir_stg./s_p";

libname raw			(raw_oth raw_bbr raw_bpe raw_car raw_ddm raw_rmp raw_ybc raw_ybd raw_sam raw_s_p) 	access=readonly; 
libname stg			(stg_oth stg_bbr stg_bpe stg_car stg_ddm stg_rmp stg_ybc stg_ybd stg_sam stg_s_p)	access=readonly; 	
libname fact		"&dir_fact.";

libname mart_f		"&dir_mart./final_mart";					/* finalized data mart which should have default indicator */
libname mart_s		"&dir_mart./semi_mart";						/* semi-result of mart, for example, data mart without default indicator */
libname mart_v		"&dir_mart./view_mart";						/* view on top of finalized data mart, for example, insertion of exclusion flags */
libname mart		(mart_v mart_f mart_s)	access=readonly;	/* the sequence of these individual libname in concat-libname */  

/* **************************************************************************************** */
/* You must declare variable &st_RptMth in your own project file before ran this script.	*/
/*
%let FLAG_INITIAL = NO;
%let st_RptMth	= 201701;
filename cmmn "&dir_pgm./00_COMMON";
%include cmmn (E00_DECLARE_RPTDATE.sas) /source2;
*/
/* **************************************************************************************** */

