from s3_utility import get_saved_compounds_with_pagerank, read_pickle_file_from_s3
import boto3
import pickle
import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler


sheet_name_list = ["Without outlier", "Without outlier-MS treated", "Without outlier-MS not treated",
              "With outlier", "With outlier-MStreated", "With outlier-MS not treated"]


def get_spoke_embedding(compound_type, sample, sel_sheet_index, data_path, bucket_name, ppr_file_location, pvalue_thresh=0.05):
    compounds_with_pagerank = get_saved_compounds_with_pagerank(bucket_name, ppr_file_location)
    GLM_significant_compounds_mapped_to_SPOKE = get_significant_compounds_with_disease_association(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=pvalue_thresh)
    spoke_embedding_dict = {}
    spoke_vector = 0
    entry_point_count = 0
    for index, row in GLM_significant_compounds_mapped_to_SPOKE.iterrows():
        compound_id = "Compound:" + row["spoke_identifer"]
        if compound_id in compounds_with_pagerank:
            entry_point_count += 1
            object_key = ppr_file_location + "/" + compound_id + "_dict.pickle"
            spoke_embedding_data = read_pickle_file_from_s3(bucket_name, object_key)
            spoke_vector += row["disease_coeff"]*spoke_embedding_data["embedding"]            
    spoke_embedding_dict["compound_type"] = compound_type
    spoke_embedding_dict["sample"] = sample
    spoke_embedding_dict["data_spec"] = sheet_name_list[sel_sheet_index]
    spoke_embedding_dict["total_significant_compounds"] = GLM_significant_compounds_mapped_to_SPOKE.shape[0]
    spoke_embedding_dict["total_significant_compounds_spoke_entry"] = entry_point_count
    spoke_embedding_dict["embedding"] = spoke_vector
    return spoke_embedding_dict


def get_significant_compounds_with_disease_association(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=0.05):
    if compound_type != "combined":
        if compound_type == "targeted":
            model_excel_file = os.path.join(data_path, 'GLM_result_targeted_compounds_{}_sample.xlsx'.format(sample))
            mapping_file = os.path.join(data_path, "short_chain_fatty_acid_spoke_map.csv")
        elif compound_type == "global":
            model_excel_file = os.path.join(data_path, 'GLM_result_global_compounds_{}_sample.xlsx'.format(sample))
            mapping_file = os.path.join(data_path, "global_metabolomics_compound_spoke_map.csv")

        GLM_out = pd.read_excel(model_excel_file, sheet_name=sheet_name_list[sel_sheet_index], engine='openpyxl')
        compound_spoke_map = pd.read_csv(mapping_file)
        GLM_out.dropna(subset=["model_converged_flag"], inplace=True)
        GLM_out = GLM_out[GLM_out["model_converged_flag"] == 1]
        GLM_out_significant_compounds = GLM_out[GLM_out.pvalue < pvalue_thresh]
        GLM_significant_compounds_mapped_to_SPOKE = pd.merge(GLM_out_significant_compounds, compound_spoke_map, left_on="analyte_name", right_on="name")[["disease_coeff", "spoke_identifer"]]
        GLM_significant_compounds_mapped_to_SPOKE.drop_duplicates(inplace=True)
        return GLM_significant_compounds_mapped_to_SPOKE
    else:
        GLM_out_targeted = pd.read_excel(os.path.join(data_path, 'GLM_result_targeted_compounds_{}_sample.xlsx'.format(sample)), sheet_name=sheet_name_list[sel_sheet_index], engine='openpyxl')
        compound_spoke_map_targeted = pd.read_csv(os.path.join(data_path, "short_chain_fatty_acid_spoke_map.csv"))
        GLM_out_targeted.dropna(subset=["model_converged_flag"], inplace=True)
        GLM_out_targeted = GLM_out_targeted[GLM_out_targeted["model_converged_flag"] == 1]
        GLM_out_targeted_significant_compounds = GLM_out_targeted[GLM_out_targeted.pvalue < pvalue_thresh]
        GLM_out_global = pd.read_excel(os.path.join(data_path, 'GLM_result_global_compounds_{}_sample.xlsx'.format(sample)), sheet_name=sheet_name_list[sel_sheet_index], engine='openpyxl')
        compound_spoke_map_global = pd.read_csv(os.path.join(data_path, "global_metabolomics_compound_spoke_map.csv"))
        compound_spoke_map_global.drop("CHEM_ID", axis=1, inplace=True)
        compound_spoke_map = pd.concat([compound_spoke_map_targeted, compound_spoke_map_global], ignore_index=True).drop_duplicates()
        GLM_out_global.drop("chem_id", axis=1, inplace=True)
        GLM_out_global.dropna(subset=["model_converged_flag"], inplace=True)
        GLM_out_global = GLM_out_global[GLM_out_global["model_converged_flag"] == 1]
        GLM_out_global_significant_compounds = GLM_out_global[GLM_out_global.pvalue < pvalue_thresh]
        scaler = MinMaxScaler(feature_range=(GLM_out_global_significant_compounds.disease_coeff.min(), GLM_out_global_significant_compounds.disease_coeff.max()))
        GLM_out_targeted_significant_compounds["disease_coeff_normalized"] = scaler.fit_transform(GLM_out_targeted_significant_compounds[['disease_coeff']])
        GLM_out_targeted_significant_compounds.drop("disease_coeff", axis=1, inplace=True)
        GLM_out_targeted_significant_compounds = GLM_out_targeted_significant_compounds.rename(columns={"disease_coeff_normalized": "disease_coeff"})
        GLM_out_total_significant_compounds = pd.concat([GLM_out_targeted_significant_compounds, GLM_out_global_significant_compounds], ignore_index=True).drop_duplicates(subset=["analyte_name"], keep="first")
        GLM_out_total_significant_compounds_mapped_to_SPOKE = pd.merge(GLM_out_total_significant_compounds, compound_spoke_map, left_on="analyte_name", right_on="name")[["disease_coeff", "spoke_identifer"]]
        GLM_out_total_significant_compounds_mapped_to_SPOKE.drop_duplicates(inplace=True)
        return GLM_out_total_significant_compounds_mapped_to_SPOKE


def compare_saved_ppr(MAPPING_FILE, IDENTIFIER_COLUMN, bucket_location):
    mapping_file_df = pd.read_csv(MAPPING_FILE)
    mapping_file_df[IDENTIFIER_COLUMN] = "Compound:" + mapping_file_df[IDENTIFIER_COLUMN]
    cmd = "aws s3 ls s3://{}".format(bucket_location)
    out = os.popen(cmd)
    out_list = out.read().split("\n")
    saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
    saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]
    node_list = mapping_file_df[IDENTIFIER_COLUMN].unique()
    node_list = list(set(node_list) - set(saved_compound_list_))
    return node_list
