# 使用Microsoft Graph API，列出用户的所有文件，使用制表符分隔符格式输出

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
import traceback
from rich.live import Live
from rich.console import Console
from asyncTaskExecutor import AsyncTaskExecutor
from fileBackedDeviceCodeCredential import FileBackedDeviceCodeCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 源文件或文件夹的相对路径，此处为默认值，可在运行时修改
ITEM_PATH = "/"
# 用户认证信息缓存路径，用于一段时间内免重复认证
CREDENTIAL_FILE_PATH = "userXXX.json"
# 同时处理的任务数
CONCURRENCY = 5


root_item: dict = dict()
id2Item: dict[str, dict] = dict()

# item {
#   id: str
#   name: str
#   parent_id: str
#   file: bool
#   folder: bool
#   children: [item_id, ...]
#   info: DriveItem
# }

def format_and_print_item_tree(item, prefix=""):
    """
    格式化并打印文件树，使用制表符分隔符。
    """
    if not item or not item["id"]:
        return

    for index, child_id in enumerate(item["children"]):
        is_last = (index == len(item["children"]) - 1)
        label = "└──" if is_last else "├──"
        child_item = id2Item.get(child_id)
        if not child_item:
            continue
        print(f"{prefix}{label}{child_item['name']}")
        if child_item["folder"]:
            new_prefix = prefix + ("    " if is_last else "│   ")
            format_and_print_item_tree(child_item, new_prefix)


def build_item_dict(item: DriveItem | None, parent_id: str | None = None):
    """
    构建文件或文件夹的字典表示。
    """
    temp_item = {
        "id": getattr(item, "id"),
        "name": getattr(item, "name"),
        "parent_id": parent_id,
        "file": getattr(item, "file"),
        "folder": getattr(item, "folder"),
        "children": [],
        "info": item
    }
    id2Item[temp_item["id"]] = temp_item
    return temp_item


async def traverse_and_record_items(graph_client: GraphServiceClient, item: DriveItem):
    """
    递归遍历 DriveItem 的子项，构建文件树。
    """
    if not item or not getattr(item, "id", None):
        print("无效的 DriveItem，无法遍历。")
        return
    # 先记录根项id，后续从id2Item中捞取根项
    root_item_id = getattr(item, "id")
    drive_id = getattr(item.parent_reference, "drive_id")
    # 任务计数器
    total_count = 0
    record_count = 0
    failed_count = 0

    # 使用 rich 库打印信息
    live = Live(console=Console())
    def print_status():
        live.update(f"[bold blue]总数: {total_count}[/] [bold green]已记录: {record_count}[/] [bold red]失败: {failed_count}[/]")
    # 遍历并记录项目及其子项，获取文件列表
    traverse_executor = AsyncTaskExecutor(CONCURRENCY, max_size=-1) # 不限制队列大小
    async def traverse_task_func(task):
        nonlocal total_count, record_count, failed_count
        total_count += 1
        print_status()
        item_id, parent_id = task
        try:
            item = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).get()
            # 记录当前项
            current_item = build_item_dict(item, parent_id)
            childrens = []
            # 如果是文件夹，获取其子项
            if getattr(item, "folder", None):
                result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).children.get()
                while True:
                    if result and getattr(result, "value", None):
                        for child in (result.value or []):
                            childrens.append(child.id)
                            await traverse_executor.add_task((child.id, item_id))
                    next_link = getattr(result, "odata_next_link", None) if result else None
                    if not next_link:
                        break
                    result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).children.with_url(next_link).get()
            current_item["children"] = childrens
            record_count += 1
            print_status()
        except Exception as e:
            # 打印堆栈信息
            traceback.print_exc()
            failed_count += 1
            print_status()

    traverse_executor.task_func = traverse_task_func
    live.start()
    await traverse_executor.add_task((item.id, None))
    await traverse_executor.join()
    await traverse_executor.shutdown()
    live.stop()

    global root_item
    root_item = id2Item.get(root_item_id, root_item)


async def get_drive_item_by_path(graph_client: GraphServiceClient, drive_id: str, path: str):
    """
    通过路径逐段遍历 children 来获取 DriveItem。
    返回找到的 DriveItem，否则返回 None。
    """
    try:
        normalized = (path or "").strip().strip("/")
        # 获取根 DriveItem 以拿到 root 的 item_id
        root_item = await graph_client.drives.by_drive_id(drive_id).root.get()
        if not root_item or not getattr(root_item, "id", None):
            return None

        # 空路径表示根
        if not normalized:
            return root_item

        current_id = root_item.id
        if not current_id:
            return None
        segments = [seg for seg in normalized.split("/") if seg]

        for seg in segments:
            found = None
            # 获取当前节点的所有子项（处理分页）
            if not current_id:
                return None
            result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        if getattr(child, "name", None) == seg:
                            found = child
                            break
                if found is not None:
                    break
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).children.with_url(next_link).get()

            if found is None or not getattr(found, "id", None):
                return None
            current_id = found.id

        # 返回最终节点的完整详情
        if not current_id:
            return None
        final_item = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).get()
        return final_item
    except Exception:
        # 让调用方决定如何提示错误，这里返回 None
        return None


async def main():
    """
    主函数
    """
    # 定义权限范围
    scopes = ["https://graph.microsoft.com/.default"]
    
    try:
        credential = FileBackedDeviceCodeCredential(client_id=CLIENT_ID, file_path=CREDENTIAL_FILE_PATH)
        graph_client = GraphServiceClient(credentials=credential, scopes=scopes)

        # 获取用户信息，从而找到 Drive ID
        drive = await graph_client.me.drive.get()
        if not drive or not drive.id:
            print("无法获取用户的 Drive 信息。请确保账户有 OneDrive for Business。")
            return
        
        drive_id = drive.id
        print(f"成功获取 Drive ID: {drive_id}")

    except Exception as e:
        print(f"发生错误: {e}")
        if "AADSTS700016" in str(e):
            print("认证错误: 应用标识符(CLIENT_ID)可能不正确或未在目标租户中正确配置。")
        elif "AADSTS900561" in str(e):
             print("认证错误: 设备代码认证流程未完成或已超时。")
        return

    global ITEM_PATH
    tmp_path = input(f"请输入要列出的文件或文件夹路径（默认: {ITEM_PATH}）: ")
    if tmp_path:
        ITEM_PATH = tmp_path if tmp_path.startswith('/') else '/' + tmp_path
    
    try:
        # 获取指定路径的文件或文件夹
        print(f"正在查找路径: {ITEM_PATH}")
        target_item = await get_drive_item_by_path(graph_client, drive_id, ITEM_PATH)
        if not target_item or not getattr(target_item, "id", None):
            print(f"未找到路径: {ITEM_PATH} 对应的文件或文件夹。请检查路径是否正确。")
            return
        
        print(f"找到目标项: {getattr(target_item, 'name', '未知')} (ID: {getattr(target_item, 'id', '未知')})")
    except Exception as e:
        print(f"查找路径时发生错误: {e}")
        return
    
    # 遍历并记录所有子项
    await traverse_and_record_items(graph_client, target_item)
    print("文件树构建完成。")
    
    # 格式化输出文件树
    print(root_item['name'])
    format_and_print_item_tree(root_item)


if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行认证。")
    asyncio.run(main())
