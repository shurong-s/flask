from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
from datetime import datetime
import os
from pathlib import Path
import io
from functools import lru_cache
import re
import json  # 新增：用于处理配置文件

app = Flask(__name__)
app.secret_key = "your_secret_key_here"

# 配置文件路径
CONFIG_FILE = Path(__file__).parent / "config.json"

# 默认路径配置
DEFAULT_CONFIG = {
    "SERVER_PATH": str(Path.home() / "Desktop"),
    "CABLE_PATH": "光缆",
    "PMS_FILE": "2022年-2025年系统任务清单（取单任务完成时间）",
    "SSCM_FILE": "领用申请单详情列表",
    "RESULTS_FILE": "results"
}

# 加载配置
def load_config():
    """加载配置文件，如果不存在则创建默认配置"""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 确保所有必要的配置项都存在
                for key, value in DEFAULT_CONFIG.items():
                    if key not in config:
                        config[key] = value
                return config
        except Exception as e:
            flash(f"加载配置文件失败，使用默认配置: {str(e)}", 'warning')
            return DEFAULT_CONFIG.copy()
    else:
        # 保存默认配置
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

# 保存配置
def save_config(config):
    """保存配置到文件"""
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        flash(f"保存配置失败: {str(e)}", 'error')
        return False

# 加载配置
config = load_config()

# 根据配置获取路径
def get_paths():
    """根据当前配置计算所有路径"""
    server_path = Path(config["SERVER_PATH"])
    cable_path = server_path / config["CABLE_PATH"]
    return {
        "SERVER_PATH": server_path,
        "CABLE_PATH": cable_path,
        "PMS_FILE": cable_path / config["PMS_FILE"],
        "SSCM_FILE": cable_path / config["SSCM_FILE"],
        "RESULTS_FILE": cable_path / config["RESULTS_FILE"]
    }

# 获取路径（全局变量）
paths = get_paths()

# 确保目录存在
os.makedirs(paths["CABLE_PATH"], exist_ok=True)

# 定义需要保存到result表的字段（核心配置）
REQUIRED_FIELDS = [
    "项目名称", "任务名称", "物料/组合物料描述",
    "申领数量", "创建日期", "厂家箱号", "使用数量"
]

# 全局缓存变量
cache_data = {
    "pms_df": None,
    "sscm_df": None,
    "results_df": None,
    "last_updated": 0  # 最后更新时间（时间戳）
}
CACHE_EXPIRE = 300  # 缓存过期时间（5分钟）


# 名称清洗函数
def clean_name(name):
    if pd.isna(name):
        return ""
    name = str(name).strip()
    # 保留更多可能的关键词（如斜线、括号）
    name = re.sub(r'[^\w\s\u4e00-\u9fa5/()]', '', name)  # 允许斜线和括号
    name = name.lower()
    stop_words = ['临时_', '临时-', '新建_', '新建-']
    for word in stop_words:
        if name.startswith(word):
            name = name[len(word):]
    return re.sub(r'\s+', ' ', name).strip()


# Excel转Parquet格式（首次运行时执行）
def convert_to_parquet():
    try:
        # 转换PMS文件
        if not (paths["PMS_FILE"].with_suffix('.parquet')).exists() and (paths["PMS_FILE"].with_suffix('.xlsx')).exists():
            pms_df = pd.read_excel(paths["PMS_FILE"].with_suffix('.xlsx'))
            pms_df.to_parquet(paths["PMS_FILE"].with_suffix('.parquet'), index=False)
            print(f"已转换PMS文件为Parquet格式: {paths['PMS_FILE'].with_suffix('.parquet')}")

        # 转换SSCM文件
        if not (paths["SSCM_FILE"].with_suffix('.parquet')).exists() and (paths["SSCM_FILE"].with_suffix('.xlsx')).exists():
            sscm_df = pd.read_excel(paths["SSCM_FILE"].with_suffix('.xlsx'))
            sscm_df.to_parquet(paths["SSCM_FILE"].with_suffix('.parquet'), index=False)
            print(f"已转换SSCM文件为Parquet格式: {paths['SSCM_FILE'].with_suffix('.parquet')}")

    except Exception as e:
        print(f"转换文件格式时出错: {str(e)}")


# 检查数据文件是否存在
def check_files():
    missing_files = []
    if not (paths["PMS_FILE"].with_suffix('.parquet').exists() or paths["PMS_FILE"].with_suffix('.xlsx').exists()):
        missing_files.append(f"任务清单: {paths['PMS_FILE'].with_suffix('.xlsx')} 或 {paths['PMS_FILE'].with_suffix('.parquet')}")
    if not (paths["SSCM_FILE"].with_suffix('.parquet').exists() or paths["SSCM_FILE"].with_suffix('.xlsx').exists()):
        missing_files.append(f"领用申请单: {paths['SSCM_FILE'].with_suffix('.xlsx')} 或 {paths['SSCM_FILE'].with_suffix('.parquet')}")
    return missing_files


# 预加载数据到缓存
def preload_data(force=False):
    global cache_data
    now = datetime.timestamp(datetime.now())

    # 缓存未过期且不强制刷新，直接返回
    if not force and now - cache_data["last_updated"] < CACHE_EXPIRE:
        return cache_data["pms_df"], cache_data["sscm_df"], cache_data["results_df"], None

    try:
        missing_files = check_files()
        if missing_files:
            error = "未找到以下必要文件:\n" + "\n".join(missing_files)
            return None, None, None, error

        # 读取PMS数据（优先Parquet格式）
        if paths["PMS_FILE"].with_suffix('.parquet').exists():
            pms_df = pd.read_parquet(paths["PMS_FILE"].with_suffix('.parquet'))
        else:
            pms_df = pd.read_excel(
                paths["PMS_FILE"].with_suffix('.xlsx'),
                usecols=['项目名称', '任务名称', '单任务物资平衡表完成时间'],
                engine='openpyxl'
            )

        # 处理PMS数据 - 筛选未完成任务
        pms_df = pms_df.astype({
            '项目名称': 'category',
            '任务名称': 'category'
        })
        if '单任务物资平衡表完成时间' in pms_df.columns:
            pms_df['单任务物资平衡表完成时间'] = pd.to_datetime(
                pms_df['单任务物资平衡表完成时间'],
                errors='coerce'
            )
            # 筛选未完成任务（完成时间为空）
            pms_df = pms_df[pms_df['单任务物资平衡表完成时间'].isna()].copy()
            pms_df['完成年份'] = pms_df['单任务物资平衡表完成时间'].dt.year

        # 读取SSCM数据（优先Parquet格式）
        if paths["SSCM_FILE"].with_suffix('.parquet').exists():
            sscm_df = pd.read_parquet(paths["SSCM_FILE"].with_suffix('.parquet'))
        else:
            sscm_df = pd.read_excel(
                paths["SSCM_FILE"].with_suffix('.xlsx'),
                usecols=REQUIRED_FIELDS[:-1] + ['站点名称'],  # 包含站点名称
                engine='openpyxl'
            )

        # 处理SSCM数据类型
        sscm_df = sscm_df.astype({
            '项目名称': 'category',
            '站点名称': 'category',
            '厂家箱号': 'category'
        })

        # 读取results数据
        if paths["RESULTS_FILE"].with_suffix('.parquet').exists():
            results_df = pd.read_parquet(paths["RESULTS_FILE"].with_suffix('.parquet'))
        elif paths["RESULTS_FILE"].with_suffix('.xlsx').exists():
            results_df = pd.read_excel(
                paths["RESULTS_FILE"].with_suffix('.xlsx'),
                engine='openpyxl'
            )
        else:
            # 初始化空表（包含项目编码字段）
            results_df = pd.DataFrame(columns=['项目编码'] + REQUIRED_FIELDS)
            results_df.to_excel(paths["RESULTS_FILE"].with_suffix('.xlsx'), index=False, engine='openpyxl')
            results_df.to_parquet(paths["RESULTS_FILE"].with_suffix('.parquet'), index=False)
            print(f"已创建新的结果表: {paths['RESULTS_FILE'].with_suffix('.xlsx')}")

        # 确保结果表包含项目编码字段
        if '项目编码' not in results_df.columns:
            results_df['项目编码'] = None
        results_df = results_df.reindex(columns=['项目编码'] + REQUIRED_FIELDS)
        results_df = results_df.astype({
            '项目名称': 'category',
            '项目编码': 'category',
            '任务名称': 'category',
            '厂家箱号': 'category'
        })

        # 更新缓存
        cache_data = {
            "pms_df": pms_df,
            "sscm_df": sscm_df,
            "results_df": results_df,
            "last_updated": now
        }
        return pms_df, sscm_df, results_df, None
    except Exception as e:
        return None, None, None, f"加载数据时出错: {str(e)}"


# 从缓存加载数据（支持筛选列）
def load_excel_data(usecols=None):
    pms_df, sscm_df, results_df, error = preload_data()
    if error:
        return None, None, None, error

    # 筛选需要的列
    if usecols:
        pms_df = pms_df[usecols].copy() if not pms_df.empty and all(
            col in pms_df.columns for col in usecols) else pms_df
        sscm_df = sscm_df[usecols].copy() if not sscm_df.empty and all(
            col in sscm_df.columns for col in usecols) else sscm_df
        results_df = results_df[usecols].copy() if not results_df.empty and all(
            col in results_df.columns for col in usecols) else results_df

    return pms_df, sscm_df, results_df, None


# 刷新缓存
def refresh_cache():
    global cache_data
    cache_data["last_updated"] = 0  # 强制过期
    preload_data(force=True)  # 重新加载数据
    cached_projects.cache_clear()  # 清除项目缓存
    cached_tasks.cache_clear()  # 清除任务缓存


# 缓存配置
@lru_cache(maxsize=128, typed=False)
def cached_projects(year=None):
    return get_project_list(use_cache=False, year=year)


@lru_cache(maxsize=128, typed=True)
def cached_tasks(project_name):
    return get_task_list(project_name, use_cache=False)


# 初始化结果表（程序启动时自动执行）
def initialize_results_table(force_override=False):
    # 加载必要数据列
    pms_df, sscm_df, results_df, error = load_excel_data(
        usecols=['项目名称', '任务名称', '站点名称', '物料/组合物料描述',
                 '申领数量', '创建日期', '厂家箱号', '单任务物资平衡表完成时间']
    )
    if error:
        return False, error

    try:
        # 1. 验证源数据完整性 - 增加对项目名称的容错处理
        required_pms_fields = ['任务名称', '单任务物资平衡表完成时间']
        missing_pms = [f for f in required_pms_fields if f not in pms_df.columns]

        # 查找项目名称的可能变体
        project_name_candidates = ['项目名称', '项目', '工程名称', '工程']
        pms_project_col = None
        for candidate in project_name_candidates:
            if candidate in pms_df.columns:
                pms_project_col = candidate
                break
        if pms_project_col is None:
            missing_pms.append("项目名称（或类似字段）")

        if missing_pms:
            return False, f"PMS数据缺少必要字段: {', '.join(missing_pms)}"

        # 对SSCM数据做同样处理
        required_sscm_fields = ['站点名称', '物料/组合物料描述', '申领数量', '创建日期', '厂家箱号']
        missing_sscm = [f for f in required_sscm_fields if f not in sscm_df.columns]

        sscm_project_col = None
        for candidate in project_name_candidates:
            if candidate in sscm_df.columns:
                sscm_project_col = candidate
                break
        if sscm_project_col is None:
            missing_sscm.append("项目名称（或类似字段）")

        if missing_sscm:
            return False, f"SSCM数据缺少必要字段: {', '.join(missing_sscm)}"

        # 2. 筛选PMS中未完成的任务（完成时间为空）
        pms_unfinished = pms_df[pms_df['单任务物资平衡表完成时间'].isna()].copy()
        if pms_unfinished.empty:
            return False, "PMS中没有未完成的任务（单任务物资平衡表完成时间为空的记录）"

        # 3. 数据清洗 - 区分编码和中文名称
        pms_unfinished['清洗后的项目名称'] = pms_unfinished[pms_project_col].apply(clean_name)
        pms_unfinished['清洗后的任务名称'] = pms_unfinished['任务名称'].apply(clean_name)
        pms_unfinished['pms_project_name'] = pms_unfinished[pms_project_col]  # 保留PMS中文名称

        sscm_df['清洗后的项目编码'] = sscm_df[sscm_project_col].apply(clean_name)  # SSCM编码清洗
        sscm_df['清洗后的站点名称'] = sscm_df['站点名称'].apply(clean_name)
        sscm_df['sscm_project_code'] = sscm_df[sscm_project_col]  # 保留SSCM原始编码

        # 4. 匹配逻辑：用SSCM编码匹配PMS中文名称
        merged_df = pd.merge(
            sscm_df,
            pms_unfinished,
            left_on=["清洗后的项目编码", "清洗后的站点名称"],
            right_on=["清洗后的项目名称", "清洗后的任务名称"],
            how="inner"
        )

        # 5. 检查匹配结果
        if merged_df.empty:
            pms_projects = pms_unfinished['清洗后的项目名称'].unique()
            sscm_projects = sscm_df['清洗后的项目编码'].unique()
            common_projects = set(pms_projects) & set(sscm_projects)
            return False, (f"未找到匹配的记录！\n"
                           f"PMS未完成任务的项目: {list(pms_projects[:5])}...\n"
                           f"SSCM的项目编码: {list(sscm_projects[:5])}...\n"
                           f"共同项: {list(common_projects) or '无'}")

        # 6. 构造结果表（包含项目编码）
        # 查找任务名称列
        task_name_candidates = ['任务名称', '任务', '站点名称', '站点']
        task_name_col = None
        for candidate in task_name_candidates:
            if candidate in merged_df.columns:
                task_name_col = candidate
                break
        if task_name_col is None:
            task_cols = [col for col in merged_df.columns if '任务' in col or '站点' in col]
            if task_cols:
                task_name_col = task_cols[0]
            else:
                return False, "无法找到任务名称或站点名称相关字段"

        # 构造结果数据
        new_results = pd.DataFrame({
            '项目名称': merged_df['pms_project_name'],  # PMS中文名称
            '项目编码': merged_df['sscm_project_code'],  # SSCM编码
            '任务名称': merged_df[task_name_col],
            '物料/组合物料描述': merged_df['物料/组合物料描述'] if '物料/组合物料描述' in merged_df.columns else
            merged_df[[col for col in merged_df.columns if '物料' in col][0]],
            '申领数量': merged_df['申领数量'] if '申领数量' in merged_df.columns else merged_df[
                [col for col in merged_df.columns if '数量' in col and '申领' in col][0]],
            '创建日期': merged_df['创建日期'] if '创建日期' in merged_df.columns else merged_df[
                [col for col in merged_df.columns if '日期' in col][0]],
            '厂家箱号': merged_df['厂家箱号'] if '厂家箱号' in merged_df.columns else merged_df[
                [col for col in merged_df.columns if '箱号' in col or 'SN' in col][0]],
            '使用数量': None
        })

        # 确保字段顺序正确
        new_results = new_results[['项目编码'] + REQUIRED_FIELDS]

        # 7. 按创建日期排序
        new_results['创建日期'] = pd.to_datetime(new_results['创建日期'], errors='coerce')
        new_results = new_results.sort_values(by='创建日期', ascending=True, na_position='last')

        # 8. 去重处理
        if not force_override and not results_df.empty:
            existing_sn = set(results_df["厂家箱号"].dropna().unique())
            new_results = new_results[~new_results["厂家箱号"].isin(existing_sn)]
            updated_results = pd.concat([results_df, new_results], ignore_index=True)
            new_count = len(new_results)
        else:
            updated_results = new_results
            new_count = len(updated_results)

        # 9. 保存结果
        try:
            excel_path = paths["RESULTS_FILE"].with_suffix('.xlsx')
            updated_results.to_excel(excel_path, index=False, engine='openpyxl')
            parquet_path = paths["RESULTS_FILE"].with_suffix('.parquet')
            updated_results.to_parquet(parquet_path, index=False)
        except Exception as save_error:
            return False, f"保存结果表失败: {str(save_error)}"

        refresh_cache()
        return True, f"成功导入{len(updated_results)}条记录（新增{new_count}条，按创建日期排序）"
    except Exception as e:
        import traceback
        return False, f"初始化失败: {str(e)}\n详细错误: {traceback.format_exc()}"


# 程序启动时自动初始化结果表
def auto_initialize_on_startup():
    # 检查结果表是否已存在且有数据
    if paths["RESULTS_FILE"].with_suffix('.xlsx').exists():
        try:
            existing_df = pd.read_excel(paths["RESULTS_FILE"].with_suffix('.xlsx'))
            if not existing_df.empty:
                print("结果表已存在且有数据，跳过自动初始化")
                return
        except:
            pass  # 若文件损坏则重新初始化

    # 执行初始化
    print("程序启动中，自动初始化结果表...")
    success, message = initialize_results_table(force_override=True)
    if success:
        print(f"初始化成功: {message}")
    else:
        print(f"初始化警告: {message}（可在系统中手动处理）")


# 获取待处理项目列表
def get_pending_projects():
    pms_df, sscm_df, _, error = load_excel_data(
        usecols=['项目名称', '任务名称', '单任务物资平衡表完成时间', '站点名称']
    )
    if error or pms_df is None or sscm_df is None:
        return [], error

    try:
        # 清洗名称
        pms_df['清洗后的项目名称'] = pms_df['项目名称'].apply(clean_name)
        pms_df['清洗后的任务名称'] = pms_df['任务名称'].apply(clean_name)
        sscm_df['清洗后的项目编码'] = sscm_df['项目名称'].apply(clean_name)
        sscm_df['清洗后的站点名称'] = sscm_df['站点名称'].apply(clean_name)

        # 合并未完成任务与领用记录
        merged_df = pd.merge(
            pms_df,
            sscm_df,
            left_on=["清洗后的项目名称", "清洗后的任务名称"],
            right_on=["清洗后的项目编码", "清洗后的站点名称"],
            how="inner"
        )

        # 提取唯一项目名称
        projects = merged_df["项目名称_x"].dropna().unique().tolist()
        return sorted(projects), None
    except Exception as e:
        return [], f"获取项目列表时出错: {str(e)}"


# 获取所有项目列表
def get_project_list(use_cache=True, year=None):
    if use_cache and year is None:
        return cached_projects()

    pms_df, _, _, error = load_excel_data(usecols=['项目名称', '完成年份'])
    if error or pms_df is None:
        return [], error

    try:
        pms_df['清洗后的项目名称'] = pms_df['项目名称'].apply(clean_name)

        # 按年份筛选（如果指定）
        if year and '完成年份' in pms_df.columns:
            mask = pms_df['完成年份'] == year
            filtered_df = pms_df[mask]
        else:
            filtered_df = pms_df

        # 去重并返回原始名称
        unique_projects = filtered_df.drop_duplicates('清洗后的项目名称')['项目名称'].dropna().tolist()
        return sorted(unique_projects), None
    except Exception as e:
        return [], f"获取项目列表时出错: {str(e)}"


# 获取指定项目的任务列表
def get_task_list(project_name, use_cache=True):
    if use_cache:
        return cached_tasks(project_name)

    pms_df, sscm_df, _, error = load_excel_data(usecols=['项目名称', '任务名称', '站点名称'])
    if error or pms_df is None or sscm_df is None:
        return [], error

    try:
        # 清洗名称
        cleaned_project = clean_name(project_name)
        pms_df['清洗后的项目名称'] = pms_df['项目名称'].apply(clean_name)
        pms_df['清洗后的任务名称'] = pms_df['任务名称'].apply(clean_name)
        sscm_df['清洗后的项目编码'] = sscm_df['项目名称'].apply(clean_name)
        sscm_df['清洗后的站点名称'] = sscm_df['站点名称'].apply(clean_name)

        # 筛选指定项目
        pms_mask = pms_df["清洗后的项目名称"] == cleaned_project
        project_pms_tasks = pms_df.loc[pms_mask, ["清洗后的任务名称", "任务名称"]]

        sscm_mask = sscm_df["清洗后的项目编码"] == cleaned_project
        project_sscm_sites = sscm_df.loc[sscm_mask, ["清洗后的站点名称"]]

        # 匹配任务和站点
        matched_tasks = pd.merge(
            project_pms_tasks,
            project_sscm_sites,
            left_on="清洗后的任务名称",
            right_on="清洗后的站点名称",
            how="inner"
        )["任务名称"].unique().tolist()

        return sorted(matched_tasks), None
    except Exception as e:
        return [], f"获取任务列表时出错: {str(e)}"


# 保存光缆使用数量
def save_usage(project_name, task_name, sn, initial_meter, end_meter):
    """保存光缆使用数量，确保只保存指定字段"""
    _, _, results_df, error = load_excel_data(usecols=['项目编码'] + REQUIRED_FIELDS)
    if error or results_df is None:
        return False, error

    try:
        # 验证输入
        usage_quantity = end_meter - initial_meter
        if usage_quantity <= 0:
            return False, "结束米标必须大于初始米标"

        # 查找并更新记录
        sn_mask = results_df["厂家箱号"] == sn
        if sn_mask.any():
            results_df.loc[sn_mask, "使用数量"] = usage_quantity
            message = f"已更新SN码 {sn} 的使用数量为 {usage_quantity:.2f} 米"
        else:
            # 从SSCM获取信息（仅获取需要的字段）
            _, sscm_df, _, error = load_excel_data(
                usecols=["项目名称", "站点名称", "物料/组合物料描述", "申领数量", "创建日期", "厂家箱号"]
            )
            if error or sscm_df is None:
                return False, error

            sscm_mask = sscm_df["厂家箱号"] == sn
            if not sscm_mask.any():
                return False, f"未找到SN码为 {sn} 的记录"

            # 添加新记录（严格使用指定字段）
            sscm_record = sscm_df.loc[sscm_mask].iloc[0]
            new_record = {
                "项目名称": project_name,
                "项目编码": sscm_record["项目名称"],  # 保存SSCM的编码
                "任务名称": task_name,
                "物料/组合物料描述": sscm_record["物料/组合物料描述"],
                "申领数量": sscm_record["申领数量"],
                "创建日期": sscm_record["创建日期"],
                "厂家箱号": sn,
                "使用数量": usage_quantity
            }

            # 确保新记录字段完整
            missing_fields = [f for f in ['项目编码'] + REQUIRED_FIELDS if f not in new_record]
            if missing_fields:
                return False, f"新记录缺少必要字段: {', '.join(missing_fields)}"

            results_df = pd.concat([results_df, pd.DataFrame([new_record])], ignore_index=True)
            message = f"已添加新记录，SN码 {sn} 的使用数量为 {usage_quantity:.2f} 米"

        # 严格筛选保存的字段
        results_df = results_df[['项目编码'] + REQUIRED_FIELDS].copy()

        # 保存数据
        results_df.to_excel(paths["RESULTS_FILE"].with_suffix('.xlsx'), index=False, engine='openpyxl')
        results_df.to_parquet(paths["RESULTS_FILE"].with_suffix('.parquet'), index=False)

        # 增强缓存刷新：不仅刷新缓存时间，直接重置数据
        global cache_data
        cache_data["results_df"] = results_df  # 直接更新缓存中的结果表
        refresh_cache()  # 触发全局缓存刷新

        return True, message
    except Exception as e:
        return False, f"保存使用数量时出错: {str(e)}"


# 获取指定项目的数据
def get_project_data(project_name):
    """获取指定项目的数据，支持中文名称和编码双重匹配"""
    # 加载数据时包含新增的项目编码字段
    _, _, results_df, error = load_excel_data(usecols=['项目编码'] + REQUIRED_FIELDS)
    if error or results_df is None:
        return None, None, error

    try:
        # 1. 增强匹配容错性：同时处理中文名称和编码
        results_df['清洗后的项目名称'] = results_df['项目名称'].apply(clean_name)
        cleaned_project = clean_name(project_name)

        # 将各列转换为字符串后再进行包含判断，得到numpy布尔数组
        name_contains = results_df["项目名称"].astype(str).str.contains(project_name, na=False, case=False).values
        cleaned_name_contains = results_df["清洗后的项目名称"].str.contains(cleaned_project, na=False).values
        code_contains = results_df["项目编码"].astype(str).str.contains(project_name, na=False, case=False).values

        # 使用numpy的布尔数组进行逻辑或运算
        mask = name_contains | cleaned_name_contains | code_contains
        filtered_data = results_df.loc[mask].copy()

        total_count = len(filtered_data)
        if total_count == 0:
            # 调试：输出不匹配的详细原因
            print(f"未匹配到数据 - 原始筛选值: {project_name}")
            print(f"结果表中的中文名称: {results_df['项目名称'].unique()[:5]}...")
            print(f"结果表中的编码: {results_df['项目编码'].unique()[:5]}...")
            return [], 0, None

        # 2. 排序逻辑
        try:
            filtered_data["创建日期"] = pd.to_datetime(filtered_data["创建日期"], errors='coerce')
            filtered_data = filtered_data.sort_values(by="创建日期", ascending=True, na_position='last')
        except Exception as e:
            app.logger.warning(f"排序失败，使用原始顺序: {str(e)}")

        # 3. 返回数据（包含项目编码用于显示）
        display_data = filtered_data[['项目编码'] + REQUIRED_FIELDS].head(10)
        return display_data.to_dict('records'), total_count, None
    except Exception as e:
        return None, None, f"获取项目数据时出错: {str(e)}"


# 导出项目数据
def export_project_data(project_name):
    """导出完整数据，包含PMS表所有列和results表所有列"""
    # 加载完整数据（不限制列，获取所有字段）
    pms_df, _, results_df, error = load_excel_data(usecols=None)  # 关键修改：不筛选列，获取全部
    if error or results_df is None or pms_df is None:
        return None, error

    try:
        # 1. 处理PMS表的任务名称字段（支持变体）
        task_candidates = ['任务名称', '任务', '站点名称', '站点']
        pms_task_col = None
        for col in task_candidates:
            if col in pms_df.columns:
                pms_task_col = col
                break
        if pms_task_col is None:
            return None, f"PMS表中未找到任务相关字段（可能的字段：{', '.join(task_candidates)}）"

        # 统一PMS表的任务字段名为“任务名称”，确保合并键一致
        if pms_task_col != '任务名称':
            pms_df = pms_df.rename(columns={pms_task_col: '任务名称'})
            print(f"PMS表字段 '{pms_task_col}' 已重命名为 '任务名称'")

        # 2. 确保结果表有“任务名称”字段
        if '任务名称' not in results_df.columns:
            return None, "结果表（results）中缺少 '任务名称' 字段"

        # 3. 筛选结果表中匹配的项目数据
        results_df['清洗后的项目名称'] = results_df['项目名称'].apply(clean_name)
        cleaned_project = clean_name(project_name)

        # 构建筛选条件
        name_contains = results_df["项目名称"].astype(str).str.contains(project_name, na=False, case=False).values
        cleaned_name_contains = results_df["清洗后的项目名称"].str.contains(cleaned_project, na=False).values
        code_contains = results_df["项目编码"].astype(str).str.contains(project_name, na=False,
                                                                        case=False).values if '项目编码' in results_df.columns else False

        mask = name_contains | cleaned_name_contains | code_contains
        filtered_results = results_df.loc[mask].copy()

        if filtered_results.empty:
            return None, f"没有找到项目 '{project_name}' 的记录"

        # 4. 合并PMS表和结果表（保留所有列）
        if '项目名称' not in pms_df.columns:
            return None, "PMS表中缺少 '项目名称' 字段"

        # 关键修改：合并时保留两个表的所有列（通过后缀区分重复列）
        merged_data = pd.merge(
            pms_df,
            filtered_results,
            on=["项目名称", "任务名称"],  # 基于共同字段合并
            how="right",  # 保留结果表所有记录
            suffixes=('_pms', '_results')  # 区分两个表的重复列（如存在相同列名）
        )

        # 5. 排序（保持原有逻辑）
        try:
            merged_data["创建日期"] = pd.to_datetime(merged_data["创建日期"], errors='coerce')
            merged_data = merged_data.sort_values(by="创建日期", ascending=True, na_position='last')
        except Exception as e:
            print(f"排序警告: {str(e)}，使用原始顺序")

        # 6. 生成Excel文件（包含所有列）
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl', mode='w') as writer:
            merged_data.to_excel(writer, index=False)  # 不排除任何列

        output.seek(0)
        return output, None
    except Exception as e:
        return None, f"导出数据时出错: {str(e)}"


# 新增：路径配置路由
@app.route('/settings', methods=['GET', 'POST'])
def settings():
    global config, paths
    
    # 计算当前路径信息（用于在页面上显示完整路径）
    current_paths = get_paths()  # 使用已定义的get_paths()函数获取完整路径
    
    if request.method == 'POST':
        # 保存配置
        new_config = {
            "SERVER_PATH": request.form.get('SERVER_PATH', DEFAULT_CONFIG['SERVER_PATH']),
            "CABLE_PATH": request.form.get('CABLE_PATH', DEFAULT_CONFIG['CABLE_PATH']),
            "PMS_FILE": request.form.get('PMS_FILE', DEFAULT_CONFIG['PMS_FILE']),
            "SSCM_FILE": request.form.get('SSCM_FILE', DEFAULT_CONFIG['SSCM_FILE']),
            "RESULTS_FILE": request.form.get('RESULTS_FILE', DEFAULT_CONFIG['RESULTS_FILE'])
        }
        
        if save_config(new_config):
            # 更新配置并刷新路径
            config = new_config
            paths = get_paths()  # 更新全局路径变量
            current_paths = paths  # 更新当前路径信息
            
            # 确保目录存在
            os.makedirs(paths["CABLE_PATH"], exist_ok=True)
            
            # 刷新缓存
            refresh_cache()
            
            flash("配置已成功保存", 'success')
            return redirect(url_for('settings'))
        else:
            flash("保存配置失败，请重试", 'error')
    
    # 渲染模板时传递config、paths和now变量
    return render_template('settings.html',
                          config=config,
                          paths=current_paths,  # 关键：传递路径信息
                          now=datetime.now())


# 路由定义
@app.route('/')
def index():
    return redirect(url_for('input_page'))


@app.route('/input')
def input_page():
    projects, error = get_pending_projects()
    selected_project = request.args.get('project', '')  # 接收URL中的项目参数
    if error:
        flash(f"加载项目失败: {error}", 'error')
        projects = []
    # 传递选中的项目到模板
    return render_template('input.html',
                          projects=projects,
                          selected_project=selected_project,  # 新增参数
                          now=datetime.now())


@app.route('/display')
def display_page():
    projects, error = get_pending_projects()
    if error:
        flash(f"加载项目失败: {error}", 'error')
        projects = []

    selected_project = request.args.get('project', '')
    data = []
    total_count = 0
    data_error = None
    if selected_project:
        data, total_count, data_error = get_project_data(selected_project)
        if data_error:
            flash(data_error, 'error')
            data = []
            total_count = 0
        if not data and total_count == 0:
            flash(f"未找到项目 '{selected_project}' 的数据记录", 'warning')

    # 确保传递now变量
    return render_template('display.html',
                           projects=projects,
                           selected_project=selected_project,
                           data=data,
                           total_count=total_count,
                           now=datetime.now())  # 关键：添加now参数


@app.route('/get_tasks/<project_name>')
def get_tasks(project_name):
    tasks, error = get_task_list(project_name)
    if error:
        return {'success': False, 'error': error}
    return {'success': True, 'tasks': tasks}


@app.route('/calculate', methods=['POST'])
def calculate():
    try:
        project = request.form['project']  # 获取当前项目名称
        task = request.form['task']
        sn = request.form['sn']
        initial_meter = float(request.form['initial_meter'])
        end_meter = float(request.form['end_meter'])

        success, message = save_usage(project, task, sn, initial_meter, end_meter)
        if success:
            flash(message, 'success')
            # 成功时跳转到该项目的display页面
            return redirect(url_for('display_page', project=project))
        else:
            flash(message, 'error')

    except ValueError:
        flash("米标必须是有效的数字", 'error')
    except Exception as e:
        flash(f"计算时出错: {str(e)}", 'error')

    # 失败时返回input页面并保留项目选择
    return redirect(url_for('input_page', project=request.args.get('project', '')))


@app.route('/export')
def export():
    project = request.args.get('project', '')
    if not project:
        flash("请先选择项目", 'error')
        return redirect(url_for('display_page'))

    output, error = export_project_data(project)
    if error:
        flash(error, 'error')
        return redirect(url_for('display_page'))

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{project}_光缆使用数据.xlsx'
    )


# 首次运行时执行格式转换
convert_to_parquet()

# 启动时自动初始化结果表
auto_initialize_on_startup()

if __name__ == '__main__':
    print(f"结果表路径: {paths['RESULTS_FILE'].with_suffix('.xlsx')}")
    app.run(debug=True)
