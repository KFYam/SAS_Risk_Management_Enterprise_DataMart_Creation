
%macro mergeDelta(key=, src=, delta=, out=, flag_ind=);
	proc sort data=&src. out=tmp; by &key.; run;
	data &out.;
		merge tmp(in=a) &delta.(in=b);
		by &key.;
		if a;
		%if &flag_ind. ne %then %do;
		if b then &flag_ind.&key.=1;
		%end;
	run;
%mend;

/* ************************************************************************************************* */
/* Target to get CIN from CAR table to map with the CIN shown in RMP file */
/* ************************************************************************************************* */
%macro getCIN_from_CAR(stdte=);
	/* *************************************************************** */
	/* Step 1 - prepare the ID-CIN listing from CAR exposure file */
	data work.car_cin_id_list(keep=_id _cin);
		length _id _cin $255;
		set	raw_car.car_corpexp_&stdte.(keep=counterparty_original id)
			raw_car.car_corpexp_&stdte.(keep=counterparty_original id2 rename=(id2=id))
		;
		_id	 = catx("_", scan(strip(counterparty_original), 1, '-'), strip(id));
		_cin = strip(counterparty_original);
		where not missing(id) and not missing(counterparty_original);
	run;
	/* *************************************************************** */
	/* Step 2 - classify 1-1 and 1-many mappings  */
	proc sort data=work.car_cin_id_list out=car_cin_id_list_v1 nodupkey; 
		by _id _cin;
	run;
	data cin_id_o2o cin_id_o2m;
		set car_cin_id_list_v1; 
		by _id;
		if first._id*last._id = 1 then do;
			ind_multi_cin=.;
			output cin_id_o2o;
		end;
		else do;
			ind_multi_cin=1;
			output cin_id_o2m;
		end;
	run;
	/* *************************************************************** */
	/* Step 3 - combine many CINs into single string per each ID */
	data cin_id_o2m_fixed (drop=_cin_o);
		length _cin $255;
		retain _cin '';
		set cin_id_o2m (rename=(_cin = _cin_o));
		by _id;	
		if first._id then _cin ='';
		_cin = catx("#",_cin ,_cin_o);
		if last._id then output;
	run;
	data view_tmp/view=view_tmp;
		set cin_id_o2o cin_id_o2m_fixed;
	run;
	proc sort data=view_tmp out=cin_id_o2o_combined; 
		by _id _cin;
	run;
	/* *************************************************************** */
	/* Step 4 - transform as format for ongoing lookup */
	data fmt;
		set cin_id_o2o_combined(rename=(_cin=label _id=start)) end=eof;
		fmtname = "$carcin"; output;
		if eof then do;
		HLO = 'O'; start="**OTHER**"; label=" ";output;
		end;
	run;
	proc format cntlin=fmt; run;
%mend;
%macro gen_RMPBase_wodefault(stdte=, rmp=, out=);
	/* %let rmp=RMP2BIO5A;  %let stdte=201503;	%let out=RMP2BIO5; 	*/
	%let RMPfile = raw_rmp.&rmp._&stdte.;
	/* ************************************************************************************************* */
	/* STEP 0: Identify joint-corp and intra bank group */
	/* ************************************************************************************************* */
	%update_ManualAdj(table_name=MART_RMP, src=&RMPfile., tar=work.RMP_Base0, st_date=&stdte.);
	/* ************************************************************************************************* */
	/* STEP 1: Convert the Key of CIN and Key of ID of the RMP file */
	/* ************************************************************************************************* */
	/* generate the CIN format based on required yearmonth ; macro is stored in DQ_01_FIX_CIN.sas */
	%getCIN_from_CAR(stdte=&stdte.); 
	data work.RMP_Base1;
		set work.RMP_Base0;
		length _cin _cin_jc _id car_cin $255 ;
		%if %varexist(work.RMP_Base0, cin_joint_corp) %then %do; _cin_jc=cin_joint_corp; %end; %else %do; _cin_jc=""; %end;
		if n_bank = 43 											then region="HKG"; else region="CHN";
		if not missing(n_mainland_cin)							then _cin = catt(region,"-",put(n_mainland_cin,z11.));
		if not missing(n_hk_cin) 								then _cin = catt(region,"-",put(n_hk_cin,z11.));
		if not missing(c_id_type) or not missing(c_id_number)	then _id  = catt(region,"_",c_id_type,c_id_number);
		if not missing(n_raid) and not missing(n_archive_id)	then _raid = catt(put(n_raid,z5.),"-",put(n_archive_id,z10.));

		_cin_rmp_ori	= _cin;
		car_cin 		= strip(put(_id, $carcin.)); /* where $cin is generated from DQ_01_FIX_CIN */
		if index(strip(car_cin),"#") > 0 then sasflag_car_cin_err = 1; /* where 1 stands for multiple CIN */

		if not missing(car_cin) and length(strip(car_cin))> 5 then do;
			if substr(strip(car_cin),5,3) in ("043" "243") then do; 
				if missing(sasflag_car_cin_err) then do;
					if missing(_cin_rmp_ori) then do; 
						sasflag_cin_mismatch = 1;  /* where 1 stands for CIN in RMP is missing but found in Fermat */
						_cin = strip(car_cin); 
					end;
					else if strip(_cin_rmp_ori) ne strip(car_cin) then do;	
						sasflag_cin_mismatch = 2; /* where 2 stands for CIN in RMP is not equal to CIN in Fermat */
						_cin = strip(car_cin);
					end;
				end;
			end;
			else do;
				sasflag_car_cin_err = 2; /* where 2 stands for non NCB/NCBC cin */
			end;
		end;
		drop region;
	run;
	/* !!! For sense checking purpose only !!
		data aa; set work.RMP_Base0; where not missing (sasflag_cin_mismatch ) or not missing(sasflag_car_cin_err );run;
	*/
	/* ************************************************************************************************* */
	/* STEP 2: Count the no of records from source file (Fullset) as at Observation (Backward) YrMth */
	/* ************************************************************************************************* */
	proc sql noprint; select count(1) into: osrc_cnt from &RMPfile.; quit;
	%put [==> Sense Checking <==]: ORIGINAL SOURCE COUNT is &osrc_cnt.;
	/* ************************************************************************************************* */
	/* STEP 3.1: Retrieve the CAR Corporate Exposure */
	/* ************************************************************************************************* */
	data work.car_corp_exp;
		set raw_car.car_corpexp_&stdte.;
		where upcase(strip(corp_card_esr_only)) ne "Y"; 

		region					= substr(counterparty_original,1,3);
		counterparty_original	= strip(counterparty_original);
		length _cin _cin_jc _id _id2 $255;
		if not missing(counterparty_original)	then _cin	= counterparty_original;
		if not missing(id) 						then _id	= catt(region,"_",id);
		if not missing(id2) 					then _id2	= catt(region,"_",id2);
		if not missing(raid)					then _raid	= strip(raid);
		_cin_jc = _cin;

		drop region;
	run;

	/* ************************************************************************************************* */
	/* STEP 3.2: Aggregrate EAD pre CCF, EAD, distinct ratings by CIN and ID levels */
	/* ************************************************************************************************* */
	%macro getEAD(key=,src=,out=);
		proc sort data=&src. out=_tmp; by &key.; where not missing(&key.);run;
		data &out.(keep=&key. aggr_rating&key. aggr_ead_pre_ccf&key. aggr_ead&key. aggr_model&key.);
			length aggr_rating&key. $255	aggr_ead_pre_ccf&key. 8 	aggr_ead&key. 8		aggr_model&key. $255;
			retain aggr_rating&key. ''		aggr_ead_pre_ccf&key. 0 	aggr_ead&key. 0		aggr_model&key. '';
			set _tmp;
			by &key.;
			if first.&key. then do;
				aggr_ead_pre_ccf&key.	= 0;
				aggr_ead&key.			= 0;
				aggr_rating&key.		= '';
				aggr_model&key.			= '';
			end;
			aggr_ead_pre_ccf&key.	= sum(aggr_ead_pre_ccf&key., ead_pre_ccf);
			aggr_ead&key. 			= sum(aggr_ead&key., ead);
			if not (find(strip(aggr_rating&key.), strip(car_rating),'i') ge 1 ) then do; 
				aggr_rating&key.	= catx(',',	aggr_rating&key., car_rating);
			end;
			if not (find(strip(aggr_model&key.), strip(model_in_use),'i') ge 1 ) then do; 
				aggr_model&key.		= catx(',',	aggr_model&key., model_in_use);
			end;
			if last.&key. then output;
		run;
		proc sort 
			data=&out. 
			out=&out. 
			%if &key.=_id2 %then %do; (rename=(_id2=_id))%end;
			;
			by &key.; 
		run;
	%mend;
	%getEAD(key=_id,	 src=work.car_corp_exp,		out=work.car_aggr_id);
	%getEAD(key=_id2,	 src=work.car_corp_exp, 	out=work.car_aggr_id2);
	%getEAD(key=_cin,	 src=work.car_corp_exp, 	out=work.car_aggr_cin);
	%getEAD(key=_raid,	 src=work.car_corp_exp, 	out=work.car_aggr_raid);
	%getEAD(key=_cin_jc, src=work.car_corp_exp, 	out=work.car_aggr_cin_jc);

	/* ************************************************************************************************* */
	/* STEP 3.3: Merge all information of EADs' information by ID, ID2 and CIN levels in RMP file */
	/* ************************************************************************************************* */
	%mergeDelta(key=_cin,	 src=work.RMP_Base1, 	delta=work.car_aggr_cin,	out=work.RMP_Base2);
	%mergeDelta(key=_id, 	 src=work.RMP_Base2, 	delta=work.car_aggr_id,		out=work.RMP_Base3);
	%mergeDelta(key=_id, 	 src=work.RMP_Base3, 	delta=work.car_aggr_id2,	out=work.RMP_Base4);
	%mergeDelta(key=_raid, 	 src=work.RMP_Base4, 	delta=work.car_aggr_raid,	out=work.RMP_Base5);
	%mergeDelta(key=_cin_jc, src=work.RMP_Base5, 	delta=work.car_aggr_cin_jc,	out=work.RMP_Base6);

	data work.RMP_EAD;
		set work.RMP_Base6;
			array	t_idx{5}	$	("id","id2","cin" "raid" "cin_jc"); 
			array	t_model{5}		aggr_model_id aggr_model_id2 aggr_model_cin aggr_model_raid aggr_model_cin_jc;
			array	t_rat {5}		aggr_rating_id aggr_rating_id2 aggr_rating_cin aggr_rating_raid aggr_rating_cin_jc;
			array	t_ead {5}		aggr_ead_id aggr_ead_id2 aggr_ead_cin aggr_ead_raid aggr_ead_cin_jc;
			array	t_eadpc{5}		aggr_ead_pre_ccf_id aggr_ead_pre_ccf_id2 aggr_ead_pre_ccf_cin aggr_ead_pre_ccf_raid aggr_ead_pre_ccf_cin_jc;
			
			ead_pre_ccf		= max(of t_eadpc [*]); /* Subject to discuss using MAX exposure as the appropiate one */
			if not missing(ead_pre_ccf) then do;
				aggr_index			= whichn(ead_pre_ccf, of t_eadpc [*]);
				ead					= t_ead[aggr_index];
				car_rating			= t_rat[aggr_index];
				car_model			= t_model[aggr_index];
				ead_pre_ccf_from	= t_idx[aggr_index];
			end;
			drop aggr_index	t_idx: ;
	run;

	/* ************************************************************************************************* */
	/* STEP 4.1: Retrieve the CAR External Rating */
	/* ************************************************************************************************* */
	data work.car_exrating;
		set raw_car.car_exrating_&stdte.;
		length _cin _id _id2 $255 ex_rating $40;
		if missing(_id2) and not missing(id_number_2) and substr(strip(id_number_1),1,1) = "5" then _id2 = catt("5",id_number_2); *Special handle the BANK Head Office Code;
		if missing(_id2) and not missing(id_number_2) then _id2 = strip(id_number_2); *Note: bank code should not be added as prefix of id, otherwise, rating cannot be assigned on the same id with different regions ;
		if missing(_id) and not missing(id_number_1) then _id = strip(id_number_1);
		if missing(_cin) and not missing(entity_code) then _cin = strip(entity_code);

		if missing(ex_rating) and not missing(sp_long_term_rating) 				then ex_rating = strip(sp_long_term_rating);
		if missing(ex_rating) and not missing(sp_long_term_rating_local_ccy) 	then ex_rating = strip(sp_long_term_rating_local_ccy);
		keep _cin _id _id2 ex_rating;
	run;

	/* ************************************************************************************************* */
	/* STEP 4.2: Retrieve external ratings by CIN and ID levels respectively */
	/* ************************************************************************************************* */
	%macro getExRating(key=, src=, out=);
		/* %let key=_id2; %let out=work.ex_rating_id2; */
		proc sort 
			data=&src.(keep=&key. ex_rating) 
			out=&out.(rename=(
				ex_rating=ex_rating&key.
				%if &key.=_id2 %then %do; _id2=_id %end;
			))
			 nodupkey; 
			where not missing(&key.) and not missing(ex_rating);
			by &key.; 
		run;
	%mend;
	%getEXRating(key=_id,	src=work.car_exrating, out=work.car_exrating_id);
	%getEXRating(key=_id2,	src=work.car_exrating, out=work.car_exrating_id2);
	%getEXRating(key=_cin,	src=work.car_exrating, out=work.car_exrating_cin);
	
	/* ************************************************************************************************* */
	/* STEP 4.3: Merge all information of Ex Rating information by ID, ID2 and CIN levels */
	/* ************************************************************************************************* */
	data work.RMP_EAD_EXRAT0;
		set work.RMP_EAD (rename=(_id=_id_ori));
		length _id $255;
		if not missing(_id_ori) then _id= strip(scan(_id_ori,-1,'_'));	/* id without region prefix is prepared for later external rating mapping */
	run;
	%mergeDelta(key=_id,	src=work.RMP_EAD_EXRAT0,	delta=work.car_exrating_id,  out=work.RMP_EAD_EXRAT1);
	%mergeDelta(key=_id,	src=work.RMP_EAD_EXRAT1,	delta=work.car_exrating_id2, out=work.RMP_EAD_EXRAT2);
	%mergeDelta(key=_cin,	src=work.RMP_EAD_EXRAT2,	delta=work.car_exrating_cin, out=work.RMP_EAD_EXRAT3);

	data work.RMP_EAD_EXRAT4(rename=(_id_ori=_id));
		set RMP_EAD_EXRAT3(rename=(_id=_id_exrating));
		length ex_rating $40;
		if missing(ex_rating) and not missing(ex_rating_id)  then ex_rating = ex_rating_id;
		if missing(ex_rating) and not missing(ex_rating_id2) then ex_rating = ex_rating_id2;
		if missing(ex_rating) and not missing(ex_rating_cin) then ex_rating = ex_rating_cin;
	run;
	/* ************************************************************************************************* */
	/* STEP 5.1: Check with RSME file by using cinid and n_hk_cin to identify the obligor in RMP file */
	/* ************************************************************************************************* */
	%macro getRSMEkey(key=, out=);
		data work.bpe_rsme /view=work.bpe_rsme;
			length _id _cin $255;
			set raw_bpe.bpe_rsme004_&stdte.;

			if Bank = "043" 			then region="HKG"; else region="CHN";
			if not missing(cif)			then _cin = catt(region,"-",strip(cif));
			if not missing(cinid) 		then _id  = catt(region,"_",strip(cinid));
			drop region;
		run;
		proc sort data=work.bpe_rsme out=&out.(keep=&key.) nodupkey; 
			by &key.; 
		run;
	%mend;
	%getRSMEkey(key=_cin,	out=work.key_resme_cin);
	%getRSMEkey(key=_id, 	out=work.key_resme_id);

	/* ************************************************************************************************* */
	/* STEP 5.2: Merge RSME indicator into RMP Base file */
	/* ************************************************************************************************* */
	%mergeDelta(key=_cin, src=RMP_EAD_EXRAT4, 			delta=work.key_resme_cin, out=work.RMP_EAD_EXRAT_RSME1, flag_ind=sasflag_rsme);
	%mergeDelta(key=_id,  src=work.RMP_EAD_EXRAT_RSME1, delta=work.key_resme_id,  out=work.RMP_EAD_EXRAT_RSME2, flag_ind=sasflag_rsme);
	data mart_s.&out._&stdte.;
		set work.RMP_EAD_EXRAT_RSME2;
		sasflag_rsme_ind = max(sasflag_rsme_cin, sasflag_rsme_id);
		/* drop sasflag_rsme_n_hk_cin sasflag_rsme_c_id; */
	run;
%mend;

%macro gen_RMPBase_wdefault(stdte=, rmp=, out=);
	%let RMPfile = raw_rmp.&rmp._&stdte.;
	/* ************************************************************************************************* */
	/* STEP 6.1: Merge the 1st default information after the observation month */
	/* ************************************************************************************************* */
	%macro getDDM(key=, src=, out=);
		%if %sysfunc(exist(mart.DDM&key._AFTER_&stdte.)) %then %do;
			proc sort 
				data=mart.DDM&key._AFTER_&stdte.(keep=
					d_1st_default_start_date c_all_default_type key&key.
				)  
				out=tmpddm(rename=(
					key&key.=&key.
					d_1st_default_start_date	=d_1st_default_start_date&key.
					c_all_default_type			=c_all_default_type&key.
				))
				nodupkey; 
				by key&key.; 
			run;
			%mergeDelta(key=&key., src=&src., delta=tmpddm, out=&out., flag_ind=sasflag_ddm);
		%end;
		%else %do;
			data &out.; set &src.; run;
		%end;	
	%mend;
	%getDDM(key=_cin, src=mart_s.&out._&stdte., 		out=work.RMP_EAD_EXRAT_RSME_DDM1);
	%getDDM(key=_id,  src=work.RMP_EAD_EXRAT_RSME_DDM1,	out=work.RMP_EAD_EXRAT_RSME_DDM2);
		
	data mart_f.&out._&stdte.;
		set work.RMP_EAD_EXRAT_RSME_DDM2;
		length sasflag_ddm_cin sasflag_ddm_id sasflag_ddm_ind 8;
		sasflag_ddm_ind = max(sasflag_ddm_cin, sasflag_ddm_id);
		label d_1st_default_start_date_cin = "";
		label d_1st_default_start_date_id = "";
		label c_all_default_type_cin="";
		label c_all_default_type_id="";
	run;
%mend;

%macro gen_RMPBase(stdte=, rmp=, out=);
	%if %sysfunc(exist(mart_s.&out._&stdte.))  %then %do;	
		%put ---- NCB LOG ---- : !! mart_s.&out._&stdte. is already existed.;
	%end;
	%else %do;
		%gen_RMPBase_wodefault(stdte=&stdte., rmp=&rmp., out=&out.);
	%end;

	%if %sysfunc(exist(mart_f.&out._&stdte.))  %then %do;	
		%put ---- NCB LOG ---- : !! mart_f.&out._&stdte. is already existed.;
	%end;
	%else %do;
		%if %sysfunc(exist(mart.DDM_CIN_AFTER_&stdte.)) or %sysfunc(exist(mart.DDM_ID_AFTER_&stdte.))  %then %do;	
			%gen_RMPBase_wdefault(stdte=&stdte., rmp=&rmp., out=&out.);
		%end;
		%else %do;
			%put ---- NCB LOG ---- : !! mart_f.&out._&stdte. cannot be generated because of lack of mart.DDM_CIN_AFTER_&stdte. and mart.DDM_ID_AFTER_&stdte..;
		%end;
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=RMP2BIOLCM_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOLCM,			out=RMP2BIOLCM);  		*03 LC model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOLCM,			out=RMP2BIOLCM);  		*03 LC model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIOMM_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOMM,			out=RMP2BIOMM); 		*04 MM model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOMM,			out=RMP2BIOMM); 		*04 MM model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIOREDI_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOREDI,		out=RMP2BIOREDI);  		*05 REDI model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOREDI,		out=RMP2BIOREDI);  		*05 REDI model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIOREIMS_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOREIMSA,		out=RMP2BIOREIMS); 		*06 REIMS model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOREIMSA,		out=RMP2BIOREIMS); 		*06 REIMS model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIONBFIINS_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIONBFIINSA,	out=RMP2BIONBFIINS);	*07 NBFIINS model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIONBFIINSA,	out=RMP2BIONBFIINS);	*07 NBFIINS model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIONBFISEC_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIONBFISECA,	out=RMP2BIONBFISEC);	*08 NBFISEC model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIONBFISECA,	out=RMP2BIONBFISEC);	*08 NBFISEC model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIOOF_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOOFA,			out=RMP2BIOOF);  		*09 OF model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOOFA,			out=RMP2BIOOF);  		*09 OF model;
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=RMP2BIOPF_&st_RptMth._MART_&st_cltinfo.);
%gen_RMPBase(stdte=&st_BackMth.,	rmp=RMP2BIOPFA,			out=RMP2BIOPF);  		*10 PF model;
%gen_RMPBase(stdte=&st_RptMth.,		rmp=RMP2BIOPFA,			out=RMP2BIOPF);  		*10 PF model;
%exportlog(ind=STOP);
