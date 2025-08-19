import streamlit as st
import pandas as pd

# -------------------------- 模拟数据源（实际可替换为数据库查询等） --------------------------
# 这里模拟未完成平衡的项目任务数据（超过10条，演示筛选后取10条）
task_data = [
    {"序号": 1, "任务名称": "任务1", "物料名称": "物料A", "申领数量": 10, "使用数量": 5},
    {"序号": 2, "任务名称": "任务2", "物料名称": "物料B", "申领数量": 20, "使用数量": 8},
    {"序号": 3, "任务名称": "任务3", "物料名称": "物料C", "申领数量": 15, "使用数量": 12},
    {"序号": 4, "任务名称": "任务4", "物料名称": "物料D", "申领数量": 8, "使用数量": 3},
    {"序号": 5, "任务名称": "任务5", "物料名称": "物料E", "申领数量": 12, "使用数量": 6},
    {"序号": 6, "任务名称": "任务6", "物料名称": "物料F", "申领数量": 18, "使用数量": 9},
    {"序号": 7, "任务名称": "任务7", "物料名称": "物料G", "申领数量": 25, "使用数量": 10},
    {"序号": 8, "任务名称": "任务8", "物料名称": "物料H", "申领数量": 5, "使用数量": 2},
    {"序号": 9, "任务名称": "任务9", "物料名称": "物料I", "申领数量": 30, "使用数量": 15},
    {"序号": 10, "任务名称": "任务10", "物料名称": "物料J", "申领数量": 7, "使用数量": 4},
    {"序号": 11, "任务名称": "任务11", "物料名称": "物料K", "申领数量": 9, "使用数量": 3},  # 额外数据演示筛选
]
df = pd.DataFrame(task_data)

# -------------------------- 页面1：扫码入口 --------------------------
def page_scan():
    st.set_page_config(page_title="项目管理系统", page_icon="📦", layout="centered")
    st.title("页面1：扫码入口")

    # 筛选框
    st.subheader("项目名称筛选")
    project_filter = st.text_input("输入项目名称筛选", "")
    
    st.subheader("任务名称筛选")
    task_filter = st.text_input("输入任务名称筛选", "")

    # 扫码入口按钮
    if st.button("点击进入扫码"):
        st.success("进入扫码流程...（此处可对接实际扫码逻辑，如调用摄像头等）")

    # 初始米标、结束米标扫码演示
    st.subheader("初始米标")
    if st.button("点击进入扫码 [初始米标]"):
        st.info("初始米标扫码完成，模拟数据：XYZ123")  # 实际可存数据库或变量
    st.subheader("结束米标")
    if st.button("点击进入扫码 [结束米标]"):
        st.info("结束米标扫码完成，模拟数据：ABC456")

# -------------------------- 页面2：展示数据 --------------------------
def page_data():
    st.set_page_config(page_title="项目管理系统", page_icon="📊", layout="centered")
    st.title("页面2：展示数据")

    # 筛选框 - 模拟根据需求筛选未完成平衡任务（仅展示10条）
    st.subheader("筛选框（筛选未完成平衡任务）")
    filtered_df = df  # 这里先模拟，实际可根据状态筛选，比如加条件：df[df['使用数量'] < df['申领数量']]
    
    # 仅展示10条
    filtered_df = filtered_df.head(10)
    
    # 展示表格
    st.dataframe(filtered_df, columns=["序号", "任务名称", "物料名称", "申领数量", "使用数量"], use_container_width=True)

    # 数据导出
    if st.button("支持数据导出生成链接"):
        # 保存为CSV（Streamlit自动处理临时文件下载）
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="点击下载数据",
            data=csv,
            file_name="task_data.csv",
            mime="text/csv"
        )

# -------------------------- 多页面路由 --------------------------
PAGES = {
    "页面1：扫码入口": page_scan,
    "页面2：展示数据": page_data
}

st.sidebar.title("导航")
selection = st.sidebar.radio("前往", list(PAGES.keys()))

# 渲染选中页面
page = PAGES[selection]
page()