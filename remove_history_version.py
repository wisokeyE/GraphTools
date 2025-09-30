# 使用Microsoft Graph API遍历OneDrive指定路径下的文件或文件夹，并通过网页逆向请求移除其及其子项的历史版本，释放空间

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
import aiohttp
from urllib import parse
from azure.identity import DeviceCodeCredential
from msgraph.graph_service_client import GraphServiceClient

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 文件或文件夹路径
ITEM_PATH = "/新建文件夹"

# 全局变量
id2Name = {}

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

async def remove_history_version(session: aiohttp.ClientSession, web_url: str, versionLabel: str):
    """
    使用网页逆向出来的请求移除项目的历史版本
    """
    prefix = '/'.join(web_url.split('/')[:5])
    web_url = parse.unquote('/' + '/'.join(web_url.split('/')[3:]))
    web_url = full_quote(f"'{web_url}'")
    versionLabel = full_quote(f"'{versionLabel}'")
    api_url = f"{prefix}/_api/web/GetFileByServerRelativePath(decodedUrl=@a1)/versions/RecycleByLabel(versionLabel=@a2)?@a1={web_url}&@a2={versionLabel}"
    headers = {
        # 此处的Headers需要从浏览器请求中获取，先打开F12的网络面板，然后在OneDrive网页端删除某个文件的历史版本，搜索RecycleByLabel，选中请求，复制为Fetch(Node.js),然后取出其中的Headers
    }
    # print(f"请求 URL: {api_url}")
    async with session.post(api_url, headers=headers) as resp:
        text = await resp.text()
        if '\\u' in text:
            # 尝试解码 Unicode 转义字符
            text = text.encode('utf-8').decode('unicode_escape')
        if resp.status != 200:
            raise Exception(f"请求失败，URL: {api_url}，状态码: {resp.status}, 响应内容: {text}")
        if text != '{"d":{"RecycleByLabel":null}}':
            print(f"警告: URL {api_url} 非预期响应内容: {text}")

# 遍历项及其子项
async def traverse_items(graph_client: GraphServiceClient, drive_id: str, item_id: str):
    """
    递归遍历指定项及其子项，移除文件的历史版本。
    """
    queue = [item_id]

    async with aiohttp.ClientSession() as session:
        while queue:
            current_item_id = queue.pop(0)
            need_check_versions = []
            # 获取当前项信息
            current_item = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_item_id).get()

            # 判断是否为文件夹
            if getattr(current_item, "file", None):
                # 是文件，加入需要检查历史版本的列表
                need_check_versions.append(current_item)
            if getattr(current_item, "folder", None):
                # 是文件夹，获取其子项（处理分页）
                result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_item_id).children.get()
                while True:
                    if result and getattr(result, "value", None):
                        for child in (result.value or []):
                            cid = getattr(child, "id", None)
                            if not cid:
                                continue
                            if getattr(child, "name", None):
                                id2Name[cid] = child.name
                            if getattr(child, "file", None):
                                # 是文件，加入需要检查历史版本的列表
                                need_check_versions.append(child)
                            if getattr(child, "folder", None):
                                # 入队以继续向下遍历
                                queue.append(cid)

                    next_link = getattr(result, "odata_next_link", None) if result else None
                    if not next_link:
                        break
                    result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_item_id).children.with_url(next_link).get()
            
            # 处理需要检查历史版本的文件
            for item in need_check_versions:
                versions = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).versions.get()
                if not versions or not getattr(versions, "value", None):
                    continue
                versionLabels = [float(getattr(ver, "id")) for ver in (versions.value or []) if getattr(ver, "id", None)]
                # 将版本号从大到小排序，只保留最新版本
                versionLabels.sort(reverse=True)
                for vlabel in versionLabels[1:]:
                    try:
                        print(f"正在移除文件 '{id2Name.get(item.id, item.id)}' 的历史版本 {vlabel} ... ")
                        await remove_history_version(session, getattr(item, "web_url", ""), str(vlabel))
                        print(f"移除文件 '{id2Name.get(item.id, item.id)}' 的历史版本 {vlabel} 成功")
                    except Exception as e:
                        print(f"移除文件 '{id2Name.get(item.id, item.id)}' 的历史版本 {vlabel} 失败: {e}")

    

async def get_drive_item_by_path(graph_client: GraphServiceClient, drive_id: str, path: str):
    """
    通过路径逐段遍历 children 来获取 DriveItem。
    返回找到的 DriveItem，否则返回 None。
    """
    try:
        global id2Name
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
            id2Name[found.id] = found.name

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
    主函数，用于认证并启动文件权限管理流程。
    """
    # 定义权限范围
    scopes = ["https://graph.microsoft.com/.default"]
    
    try:
        credential = DeviceCodeCredential(client_id=CLIENT_ID)
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

    try:
        # 根据路径获取文件夹的 DriveItem
        print(f"正在查找文件夹: '{ITEM_PATH}'")
        # 通过逐段遍历 children 的方式来解析路径
        target_folder = await get_drive_item_by_path(graph_client, drive_id, ITEM_PATH)
        if not target_folder or not target_folder.id:
            print(f"找不到指定的文件夹: '{ITEM_PATH}'")
            return
        
        print(f"找到文件夹 ID: {target_folder.id}")
        
    except Exception as e:
        print(f"通过路径 '{ITEM_PATH}' 查找文件夹时出错: {e}")
        print("请检查路径是否正确，以及应用是否具有足够的权限 (例如 Files.ReadWrite.All)。")
        return

    # 移除项及其子项的历史版本
    await traverse_items(graph_client, drive_id, target_folder.id)
    print("\n指定项及其子项的历史版本清理完成。")


if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行认证。请在浏览器中打开认证页面并输入以下代码。")
    asyncio.run(main())
