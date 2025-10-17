# 使用Microsoft Graph API遍历OneDrive指定路径下的文件或文件夹，并通过网页逆向请求移除其及其子项的历史版本，释放空间

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
import aiohttp
from rich.live import Live
from rich.console import Console
from urllib import parse
from asyncTaskExecutor import AsyncTaskExecutor
from fileBackedDeviceCodeCredential import FileBackedDeviceCodeCredential
from msgraph.generated.models.drive_item import DriveItem
from msgraph.graph_service_client import GraphServiceClient

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 文件或文件夹路径
ITEM_PATH = "/新建文件夹"
# 用户认证信息缓存路径，用于一段时间内免重复认证
CREDENTIAL_FILE_PATH = "userXXX.json"
# 同时处理的任务数
CONCURRENCY = 5

# 全局变量
refresh_event = asyncio.Event()
refersh_lock = asyncio.Lock()
headers = {
    # 此处的Headers需要从浏览器请求中获取，先打开F12的网络面板，然后在OneDrive网页端删除某个文件的历史版本，搜索RecycleByLabel，选中请求，复制为Fetch(Node.js),然后取出其中的Headers
}

def full_quote(s):
    s_quoted = parse.quote(s, safe='')
    
    # 手动替换 unreserved 字符
    replacements = {
        '-': '%2D',
        '.': '%2E',
        '_': '%5F',
        '~': '%7E'
    }
    
    for char, encoded in replacements.items():
        s_quoted = s_quoted.replace(char, encoded)
    
    return s_quoted


async def remove_file_versions(session: aiohttp.ClientSession, item: DriveItem, versionLabel: str, live: Live):
    """
    使用网页逆向出来的请求移除项目的历史版本
    """
    global headers
    if not item or not getattr(item, "id", None):
        print("无效的 DriveItem，无法处理。")
        return
    web_url = getattr(item, "web_url")
    prefix = '/'.join(web_url.split('/')[:5])
    new_web_url = parse.unquote('/' + '/'.join(web_url.split('/')[3:]))
    new_web_url = full_quote(f"'{new_web_url}'")
    versionLabel = full_quote(f"'{versionLabel}'")
    url = f"{prefix}/_api/web/GetFileByServerRelativePath(decodedUrl=@a1)/versions/RecycleByLabel(versionLabel=@a2)?@a1={new_web_url}&@a2={versionLabel}"
    await refresh_event.wait()
    # 保留旧的，以便验证是否已被刷新，避免重复刷新
    old_requestdigest = headers.get("x-requestdigest", "")
    async with session.post(url, headers=headers) as resp:
        text = await resp.text()
        if '\\u' in text:
            # 尝试解码 Unicode 转义字符
            text = text.encode('utf-8').decode('unicode_escape')
        if resp.status != 200:
            if resp.status == 403:
                # 令牌过期，触发刷新token，目前只处理403错误
                async with refersh_lock:
                    if old_requestdigest == headers.get("x-requestdigest", ""):
                        refresh_event.clear()
                        new_requestdigest = None
                        live.stop()
                        while not new_requestdigest:
                            new_requestdigest = await asyncio.get_event_loop().run_in_executor(None, input, "请求令牌可能已过期，请输入新的请求令牌（x-requestdigest）并回车以继续: ")
                            new_requestdigest = new_requestdigest.strip()
                            if new_requestdigest:
                                headers["x-requestdigest"] = new_requestdigest
                                refresh_event.set()
                        # 令牌刷新完成，重试请求
                        live.start()
                    await remove_file_versions(session, item, versionLabel, live)
            else:
                print(f"移除版本 {versionLabel} 失败: HTTP {resp.status} - {text}，项目name{item.name} id:{item.id} web_url:{item.web_url}")
        elif text != '{"d":{"RecycleByLabel":null}}':
            print(f"警告: URL {url} 非预期响应内容: {text}，项目name{item.name} id:{item.id} web_url:{item.web_url}")

async def traverse_and_remove_versions(graph_client: GraphServiceClient, item: DriveItem):
    """
    递归遍历项目及其子项，移除所有历史版本。
    """
    if not item or not getattr(item, "id", None):
        print("无效的 DriveItem，无法处理。")
        return
    
    drive_id = getattr(item.parent_reference, "drive_id")
    
    total_count = 0
    removed_count = 0
    no_history_count = 0
    failed_count = 0

    # 使用 rich 库打印信息
    live = Live(console=Console())
    def print_status():
        live.update(f"[bold blue]总数: {total_count}[/] [bold yellow]无历史: {no_history_count}[/] [bold green]已移除: {removed_count}[/] [bold red]失败: {failed_count}[/]")

    # 遍历项目及其子项，获取所有的文件
    traverse_executor = AsyncTaskExecutor(CONCURRENCY)
    files = []
    async def traverse_task_func(task):
        nonlocal total_count
        item = task
        # 如果是文件，添加到列表
        if getattr(item, "file", None):
            files.append(item)
            total_count += 1
            print_status()
        # 如果是文件夹，获取其子项
        if getattr(item, "folder", None):
            result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        # 文件，添加到文件列表
                        if getattr(child, "file", None):
                            files.append(child)
                            total_count += 1
                            print_status()
                        # 文件夹，添加到任务队列继续遍历
                        if getattr(child, "folder", None):
                            await traverse_executor.add_task(child)
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).children.with_url(next_link).get()
    
    traverse_executor.task_func = traverse_task_func
    live.start()
    await traverse_executor.add_task(item)
    await traverse_executor.join()
    await traverse_executor.shutdown()
    
    # 检查并移除文件的历史版本
    async with aiohttp.ClientSession() as session:
        async def remove_task_func(task):
            nonlocal removed_count, no_history_count, failed_count
            item = task
            versions = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).versions.get()
            if not versions or not getattr(versions, "value", None):
                no_history_count += 1
                print_status()
                return
            versionLabels = [float(getattr(ver, "id")) for ver in (versions.value or []) if getattr(ver, "id", None)]
            # 将版本号从大到小排序，只保留最新版本
            versionLabels.sort(reverse=True)
            if len(versionLabels) <= 1:
                no_history_count += 1
                print_status()
                return
            try:
                for vlabel in versionLabels[1:]:
                    await remove_file_versions(session, item, str(vlabel), live)
                removed_count += 1
                print_status()
            except Exception as e:
                print(f"移除文件 {getattr(item, 'name', '未知')} 的历史版本时发生错误: {e}")
                failed_count += 1
                print_status()
                return
        refresh_event.set()
        remove_executor = AsyncTaskExecutor(CONCURRENCY, remove_task_func)
        await remove_executor.add_tasks(files)
        await remove_executor.shutdown()
    live.stop()


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
    tmp_path = input(f"请输入要操作的文件或文件夹路径（默认: {ITEM_PATH}）: ")
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
    
    # 移除目标项及其子项的所有历史版本
    await traverse_and_remove_versions(graph_client, target_item)
    print("指定项目及其子项的历史版本移除完成。")

if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行认证。")
    asyncio.run(main())
