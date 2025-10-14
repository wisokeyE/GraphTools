# 使用Microsoft Graph API，从源驱动器的源路径复制文件到用户的目标路径

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
from rich.console import Console
from rich.live import Live
from asyncTaskExecutor import AsyncTaskExecutor
from azure.identity import DeviceCodeCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem
from msgraph.generated.models.folder import Folder
from msgraph.generated.models.item_reference import ItemReference
from msgraph.generated.drives.item.items.item.copy.copy_post_request_body import CopyPostRequestBody

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 源文件或文件夹的相对路径，此处为默认值，可在运行时修改
SOURCE_PATH = "/"
# 目标路径，如果目标路径不存在，脚本会自动创建，此处为默认值，可在运行时修改
TARGET_PARENT_PATH = "/"
# 冲突时的处理方式，可选 fail 、 replace ，不支持rename
CONFLICT_BEHAVIOR = "fail"
# 同时处理的任务数
CONCURRENCY = 10

id2Name = dict()

async def copy_files(client: GraphServiceClient, source_item: DriveItem, target_parent_item: DriveItem):
    """
    复制文件或文件夹
    """
    if not source_item or not source_item.id or not target_parent_item or not target_parent_item.id:
        print("源项或目标项无效，无法复制。")
        return

    source_drive_id = getattr((source_item.remote_item if source_item.remote_item else source_item).parent_reference, "drive_id")
    target_drive_id = getattr(target_parent_item.parent_reference, "drive_id")

    total_count = 0
    copying_count = 0
    copied_count = 0
    failed_count = 0

    # 使用 rich 库打印 总数，已复制，失败的数量
    live = Live(console=Console())
    def print_status():
        live.update(f"[bold blue]总数: {total_count}[/] [bold yellow]复制中: {copying_count}[/] [bold green]已复制: {copied_count}[/] [bold red]失败: {failed_count}[/]")

    # 先遍历源项及其子项，在目标项下创建对应的文件夹
    # 遍历源项 协程任务执行器
    traverse_executor = AsyncTaskExecutor(CONCURRENCY)
    waiting_copy = []
    target_children_cache = dict()  # 缓存目标文件夹的子项，避免重复请求
    async def traverse_task_func(task):
        nonlocal total_count
        item, target_parent_item = task
        target_parent_children = target_children_cache.get(target_parent_item.id, None)
        if target_parent_children is None:
            target_parent_children = dict()
            result = await client.drives.by_drive_id(target_drive_id).items.by_drive_item_id(target_parent_item.id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        if getattr(child, "folder", None):
                            target_parent_children[child.name] = child
                        id2Name[f'{target_drive_id}:{child.id}'] = child.name
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await client.drives.by_drive_id(target_drive_id).items.by_drive_item_id(target_parent_item.id).children.with_url(next_link).get()
            target_children_cache[target_parent_item.id] = target_parent_children
        # 如果源项是文件，则直接添加到待复制列表
        if getattr(item, "file", None):
            total_count += 1
            print_status()
            waiting_copy.append((item, target_parent_item))
        # 如果源项是文件夹，则在目标位置创建对应的文件夹（如果不存在），并遍历其子项
        if getattr(item, "folder", None):
            target_item = target_parent_children.get(item.name, None)
            if target_item is None:
                target_item = await client.drives.by_drive_id(target_drive_id).items.by_drive_item_id(target_parent_item.id).children.post(DriveItem(name=item.name, folder=Folder()))
            # 遍历源项的子项
            result = await client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(item.id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        if getattr(child, 'file', None):
                            # 文件，直接添加到待复制列表
                            total_count += 1
                            print_status()
                            waiting_copy.append((child, target_item))
                        if getattr(child, 'folder', None):
                            # 文件夹，添加到任务队列，继续遍历
                            await traverse_executor.add_task((child, target_item))
                        id2Name[f'{source_drive_id}:{getattr(child, "id")}'] = getattr(child, 'name')
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(item.id).children.with_url(next_link).get()
        
    traverse_executor.task_func = traverse_task_func
    
    live.start()
    await traverse_executor.add_task((source_item, target_parent_item))
    await traverse_executor.join()
    await traverse_executor.shutdown()

    # 开始将文件逐个复制到目标位置
    async def copy_task_func(task):
        nonlocal copying_count, copied_count, failed_count
        item, target_parent_item = task
        try:
            body = CopyPostRequestBody(
                name=getattr(item, "name"),
                parent_reference=ItemReference(
                    drive_id=target_drive_id,
                    id=target_parent_item.id
                ),
                additional_data={
                    "@microsoft.graph.conflictBehavior": CONFLICT_BEHAVIOR
                }
            )
            copied_file = await client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(item.id).copy.post(body)
            if copied_file and getattr(copied_file, "id", None):
                copied_count += 1
            else:
                copying_count += 1
            print_status()
        except Exception as e:
            live.console.print(f"[bold red]复制文件 {getattr(item, 'name', item.id)} -> 目标父项 {id2Name.get(f'{target_drive_id}:{target_parent_item.id}', target_parent_item.id)} 失败: {e}[/]")
            failed_count += 1
            print_status()

    copy_executor = AsyncTaskExecutor(CONCURRENCY, copy_task_func)
    await copy_executor.add_tasks(waiting_copy)
    await copy_executor.shutdown()
    live.stop()


async def get_drive_item_by_path(graph_client: GraphServiceClient, driveItem: DriveItem, relative_path: str, auto_create: bool = False):
    """
    通过路径逐段遍历 children 来获取 DriveItem。如果auto_create为True，则在路径不存在时自动创建文件夹。
    返回找到的 DriveItem，否则返回 None。
    """
    try:
        item = driveItem.remote_item if driveItem.remote_item else driveItem
        drive_id = getattr(item.parent_reference, "drive_id")
        normalized = (relative_path or "").strip().strip("/")

        # 空路径表示根
        if not normalized:
            return driveItem

        current_id = item.id
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

            # 如果开启了自动创建且found为None，则创建文件夹
            if auto_create and found is None:
                request_body = DriveItem(name=seg, folder = Folder())
                found = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).children.post(request_body)

            if found is None or not getattr(found, "id", None):
                return None
            current_id = found.id
            id2Name[f'{drive_id}:{current_id}'] = found.name

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
        credential = DeviceCodeCredential(client_id=CLIENT_ID)
        client = GraphServiceClient(credentials=credential, scopes=scopes)
        target_drive = await client.me.drive.get()
        if not target_drive or not target_drive.id:
            print("无法获取用户的OneDrive信息，请检查权限配置。")
            return

    except Exception as e:
        print(f"发生错误: {e}")
        if "AADSTS700016" in str(e):
            print("认证错误: 应用标识符(CLIENT_ID)可能不正确或未在目标租户中正确配置。")
        elif "AADSTS900561" in str(e):
             print("认证错误: 设备代码认证流程未完成或已超时。")
        return

    # 要求用户选择源项作为源相对路径的起点
    options = [{
        "label": "1. 选择我的OneDrive根目录作为起点",
        "value": await client.drives.by_drive_id(target_drive.id).root.get()
    }]
    # 获取分享给用户的项列表
    shared_items = []
    result = await client.drives.by_drive_id(target_drive.id).shared_with_me.get()
    while True:
        if result and getattr(result, 'value', None):
            shared_items.extend(getattr(result, 'value'))
        next_link = getattr(result, '@odata.nextLink', None) if result else None
        if not next_link:
            break
        result = await client.drives.by_drive_id(target_drive.id).shared_with_me.with_url(next_link).get()
    
    for i in range(len(shared_items)):
        item = shared_items[i]
        if item and item.remote_item:
            options.append({
                "label": f"{i + 2}. {item.remote_item.shared.shared_by.user.display_name} 共享给我的 {item.remote_item.name}",
                "value": item
            })

    select = input(f"请选择源项作为源路径的起点:\n" + "\n".join([opt["label"] for opt in options]) + "\n输入对应的数字: ")
    while not select.isdigit() or int(select) < 1 or int(select) > len(options):
        select = input("输入有误，请重新输入对应的数字: ")

    selected_option = options[int(select) - 1]
    global SOURCE_PATH
    tmp_path = input(f"请输入源路径 (相对于 {selected_option['value'].name} ，以 / 开头，默认为 {SOURCE_PATH}): ")
    if tmp_path:
        SOURCE_PATH = tmp_path if tmp_path.startswith("/") else "/" + tmp_path

    global TARGET_PARENT_PATH
    tmp_path = input(f"请输入目标路径 (相对于您的OneDrive根目录，以 / 开头，默认为 {TARGET_PARENT_PATH}): ")
    if tmp_path:
        TARGET_PARENT_PATH = tmp_path if tmp_path.startswith("/") else "/" + tmp_path

    try:
        # 获取源项与目标项
        print(f"正在查找源路径 '{SOURCE_PATH}' ...")
        source_item = await get_drive_item_by_path(client, selected_option['value'], SOURCE_PATH)
        if source_item is None or not source_item.id:
            print(f"未找到源路径 '{SOURCE_PATH}' ，请检查路径是否正确。")
            return

        print(f'找到源项 ID: {source_item.id}')
        
        print(f"正在查找或创建目标路径 '{TARGET_PARENT_PATH}' ...")
        target_parent_item = await get_drive_item_by_path(client, options[0]['value'], TARGET_PARENT_PATH, auto_create=True)
        if target_parent_item is None or not target_parent_item.id:
            print(f"未找到或创建目标路径 '{TARGET_PARENT_PATH}' ，请检查路径是否正确。")
            return

        print(f'找到目标项 ID: {target_parent_item.id}')

    except Exception as e:
        print(f"查找路径时发生错误: {e}")
        return

    try:
        # 开始复制
        await copy_files(client, source_item, target_parent_item)
    
    except Exception as e:
        print(f"复制文件时发生错误: {e}")
        return

if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行认证。")
    asyncio.run(main())
