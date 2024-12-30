/* ************************************************************************************************ */
/* STEP 0: Declare custom libraries for 1st default mart generation */
filename 	cmmn 	"&dir_pgm./00_COMMON";
filename	scpgm 	"&dir_pgm./02_SELFCHECK_SCODE";

/* ************************************************************************************************ */
/* STEP 1: Execute below CODE */
%include	scpgm	(S01_META_CHECK.sas) 		/source2;
%include	scpgm	(S02_FIELD_FREQ_CHECK.sas) 	/source2;

%exportlog(ind=START, path=&dir_log., name=RMP2BIOLCM_METACHECK_INIT_LOAD_&st_cltinfo.);
%SC_LoopAssess(lib=RAW_RMP, table_prefix=RMP2BIOLCM);
%exportlog(ind=STOP);

/*

option mprint;
%exportlog(ind=START, path=&dir_log., name=GEN_SELFCHECK_SUMMARY);
%gen_SelfCheck_Report_MetaSummary(src=RMP);
%gen_SelfCheck_Report_MetaSummary(src=BBR);
%gen_SelfCheck_Report_MetaSummary(src=BPE);
%gen_SelfCheck_Report_MetaSummary(src=BPE);
%gen_SelfCheck_Report_MetaSummary(src=BBR);
%gen_SelfCheck_Report_MetaSummary(src=CAR);
%gen_SelfCheck_Report_MetaSummary(src=CAW);
%gen_SelfCheck_Report_MetaSummary(src=CCS);
%gen_SelfCheck_Report_MetaSummary(src=CISVID);
%gen_SelfCheck_Report_MetaSummary(src=DDM);
%gen_SelfCheck_Report_MetaSummary(src=IPO);
%gen_SelfCheck_Report_MetaSummary(src=RBP);
%gen_SelfCheck_Report_MetaSummary(src=SA10);
%gen_SelfCheck_Report_MetaSummary(src=SAM);
%gen_SelfCheck_Report_MetaSummary(src=YBC);
%gen_SelfCheck_Report_MetaSummary(src=YBD);
%exportlog(ind=STOP);
*/

