import pandas as pd
import os

path_code = r"./"
path_root = os.path.join(path_code, "..")
path_input = os.path.join(path_root, "Input")
path_input_data = os.path.join(path_input, "2021-05-17 - discord_wish_data")
path_output = os.path.join(path_root, "Output")

# Load wish data file tracker
df_wish_tracker = pd.read_csv(os.path.join(path_input, "genshin_wish_data_tracker_anonymized.csv"), header=0)

# Currently have one file with Japanese sheet names: Genshin_Wish_History_20210311_195127.xlsx
sheet_name_rename_dict = {"イベント祈願・キャラクター": "Character Event Wish"
                          , "イベント祈願・武器": "Weapon Event Wish"
                          , "通常祈願": "Permanent Wish"
                          , "初心者向け祈願": "Novice Wishes"
                          }

# There are 4 file types with different column formats:
# A capitalized English, a condensed English, a Chinese format,
# and another format that seems to have the first column incorrect named
# Currently confirmed that the ordering of columns are consistent across all files
# For standardization, will check that the read in columns match one of these, then will rename to English Proper
all_file_column_names = {"English Proper": ['Timestamp', 'Reward Name', 'Reward Type', 'Rarity (Star)', 'Wish Count', 'Pity Count']
                         , "English Simple": ['time', 'name', 'type', 'rarity', 'total', 'within pity']
                         , "Chinese": ['时间', '名称', '类别', '星级', '总次数', '保底内']
                         , "English Other": ['Character', 'Name', 'Category', 'Star Rating', 'Total', 'Pity']
                         }

# Load data
wish_tracker_data_raw = []
for i, row in df_wish_tracker.iterrows():
    df_temp = pd.read_excel(os.path.join(path_input_data, row["file_name"]), engine="openpyxl", sheet_name=None)

    wish_tracker_data_raw.append((row, df_temp))

# print(wish_tracker_data_raw)

# Check columns and sheets of input files - have several different formats
# Check: Sheet names are consistent
# TODO: Change this into tests - currently small so I'm just manually looking over these
for row, df_dict in wish_tracker_data_raw:
    print(row["file_name"])
    print(df_dict.keys())
    all_columns = [v.columns.to_list() for k, v in df_dict.items()]
    print(all([e == all_columns[0] for e in all_columns]))
    print(all_columns[0])

# Stack all wish data
df_master = pd.DataFrame(columns=['banner_type', 'Timestamp', 'Reward Name', 'Reward Type', 'Rarity (Star)',
                                  'Wish Count', 'Pity Count', 'file_name'])

for row, dict_df in wish_tracker_data_raw:
    for k, v in dict_df.items():
        df_temp = v.copy()

        # There is one file with missing timestamps - we add the data in anyway just in case
        if row["file_name"] == "Copy_of_Genshin_Wish_History_20210302_112106_no_datestamp.xlsx":
            df_temp = df_temp.rename({"Unnamed: 0": "Timestamp"}, axis=1)

        # Rename columns after checking that the columns align with expected
        assert(df_temp.columns.to_list() in all_file_column_names.values())
        df_temp.columns = all_file_column_names["English Proper"]
        df_temp["file_name"] = row["file_name"]
        df_temp["banner_type"] = sheet_name_rename_dict.get(k, k)
        df_temp["hashed_id"] = row["hashed_id"]

        df_master = df_master.append(df_temp, ignore_index=True)

df_master = df_master.rename({"Timestamp": "timestamp"
                              , "Reward Name": "reward_name"
                              , "Reward Type": "reward_type"
                              , "Rarity (Star)": "rarity_star"
                              , "Wish Count": "wish_count"
                              , "Pity Count": "pity_count"
                              }, axis=1)
# Output file
df_master.to_csv(os.path.join(path_output, "genshin_whale_wish_data_stacked_anonymized.csv"), index=False)
