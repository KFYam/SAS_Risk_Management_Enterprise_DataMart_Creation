/* ************************************************************************************************ */
/* STEP 0: Declare custom libraries for 1st default mart generation */
filename 	cmmn 	"&dir_pgm./00_COMMON";
filename	mpgm 	"&dir_pgm./03_MART_SCODE";

/* **************************************************************** */
/* PART 1 - LOAD common modules and macro for later usages 			*/
/* **************************************************************** */
%include cmmn(E00_DECLARE_RPTDATE.sas) 	/source2;
%include cmmn(F05_MANUAL_ADJ.sas) 		/source2;

%include mpgm(M00_DDM_SUBSET.sas) 		/source2;
%include mpgm(M01_S_P_RATING.sas) 		/source2;
%include mpgm(M02_YBX_SUBSET.sas) 		/source2;

%include mpgm(M04_GEN_RMP_BASE.sas)		/source2;
%include mpgm(M05_GEN_RMP_BANKFI.sas)	/source2;

/* **************************************************************** */
/* PART 2 - RUN DIFFERENT MARTS			*/
/* **************************************************************** */
%exportlog(ind=START, path=&dir_log., name=DDM_&st_RptMth._CINID_MART_&st_cltinfo.);
	%gen_DDM_CINID_Level(key=cin, tarlib=mart_f);
	%gen_DDM_CINID_Level(key=id, tarlib=mart_f);
%exportlog(ind=STOP);

%exportlog(ind=START, path=&dir_log., name=SP_&st_RptMth._MART_&st_cltinfo.);
	%gen_SP_MART(stdte=&st_BackMth.);
	%gen_SP_MART(stdte=&st_RptMth.);
%exportlog(ind=STOP);

%exportlog(ind=START, path=&dir_log., name=YBC_&st_RptMth._CINID_MART_&st_cltinfo.);
	%gen_YBC_CINID_Level;
%exportlog(ind=STOP);

%exportlog(ind=START, path=&dir_log., name=YBD_&st_RptMth._CINID_MART_&st_cltinfo.);
	%gen_YBD_CINID_Level;
%exportlog(ind=STOP);


%gen_RMPBankFI(stdte=&st_BackMth.,	rmp=RMP2BIOBANK,		out=RMP2BIOBANK);  		*02 Bank model;
%gen_RMPBankFI(stdte=&st_RptMth.,	rmp=RMP2BIOBANK,		out=RMP2BIOBANK);  		*02 Bank model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOLCM,			out=RMP2BIOLCM);  		*03 LC model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOLCM,			out=RMP2BIOLCM);  		*03 LC model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOMM,			out=RMP2BIOMM); 		*04 MM model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOMM,			out=RMP2BIOMM); 		*04 MM model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOREDI,		out=RMP2BIOREDI);  		*05 REDI model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOREDI,		out=RMP2BIOREDI);  		*05 REDI model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOREIMSA,		out=RMP2BIOREIMS); 		*06 REIMS model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOREIMSA,		out=RMP2BIOREIMS); 		*06 REIMS model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIONBFIINSA,	out=RMP2BIONBFIINS);	*07 NBFIINS model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIONBFIINSA,	out=RMP2BIONBFIINS);	*07 NBFIINS model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIONBFISECA,	out=RMP2BIONBFISEC);	*08 NBFISEC model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIONBFISECA,	out=RMP2BIONBFISEC);	*08 NBFISEC model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOOFA,			out=RMP2BIOOF);  		*09 OF model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOOFA,			out=RMP2BIOOF);  		*09 OF model;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOPFA,			out=RMP2BIOPF);  		*10 PF model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOPFA,			out=RMP2BIOPF);  		*10 PF model;
/*
%gen_RMPBankFI(stdte=&st_BackMth.,	rmp=RMP2BIOBANK,out=RMP2BIOBANK_ALL);  	*02 Bank model - full set;
%gen_RMPBankFI(stdte=&st_RptMth.,	rmp=RMP2BIOBANK,out=RMP2BIOBANK_ALL);  	*02 Bank model - full set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO2,	out=RMP2BIO2_ALL);  	*03 LC model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO2,	out=RMP2BIO2_ALL);  	*03 LC model - using filter set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOMM,	out=RMP2BIOMM_ALL); 	*04 MM model - using full set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOMM,	out=RMP2BIOMM_ALL); 	*04 MM model - using full set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO7,	out=RMP2BIO7_ALL);  	*05 REDI model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO7,	out=RMP2BIO7_ALL);  	*05 REDI model - using filter set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO5A,	out=RMP2BIO5_ALL);  	*06 REIMS model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO5A,	out=RMP2BIO5_ALL);  	*06 REIMS model - using filter set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO12A,	out=RMP2BIO12_ALL);  	*07 NBFIINS model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO12A,	out=RMP2BIO12_ALL);  	*07 NBFIINS model - using filter set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO13A,	out=RMP2BIO13_ALL);  	*08 NBFISEC model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO13A,	out=RMP2BIO13_ALL);  	*08 NBFISEC model - using filter set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO10A,	out=RMP2BIO10_ALL);  	*09 OF model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO10A,	out=RMP2BIO10_ALL);  	*09 OF model - using filter set;
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIO9A,	out=RMP2BIO9_ALL);  	*10 PF model - using filter set;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIO9A,	out=RMP2BIO9_ALL);  	*10 PF model - using filter set;
*/

