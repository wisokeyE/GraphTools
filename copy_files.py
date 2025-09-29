# 使用Microsoft Graph API，从源OneDrive账户复制文件到目标OneDrive账户，并自动处理源文件的权限问题

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
import aiohttp
from azure.identity import DeviceCodeCredential
from azure.identity._internal.interactive import InteractiveCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem
from msgraph.generated.models.folder import Folder
from msgraph.generated.models.drive_recipient import DriveRecipient
from msgraph.generated.drives.item.items.item.invite.invite_post_request_body import InvitePostRequestBody

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 源文件或文件夹路径
SOURCE_PATH = "/新建文件夹"
# 目标路径，如果目标路径不存在，脚本会自动创建
TARGET_PARENT_PATH = "/"
# 冲突时的处理方式，可选 fail 、 replace ，不支持rename
CONFLICT_BEHAVIOR = "fail"
# 最大单次复制项目大小，对于文件夹总大小小于该值的，脚本会直接使用copy api进行复制，对于文件夹总大小超过该值的，则会遍历其子项继续进行判断，对于文件，不判断大小直接使用copy api进行复制
MAX_COPY_SIZE = 100 * 1024 * 1024 * 1024 # 100GB
# 同时处理的复制任务数
CONCURRENCY = 5
id2Name = dict()

def get_access_token(credential: InteractiveCredential) -> str:
    token_obj = credential.get_token("https://graph.microsoft.com/.default")
    return getattr(token_obj, "token", str(token_obj))
    

async def copy_item(session: aiohttp.ClientSession, target_credential: InteractiveCredential, source_drive_id: str, source_item_id: str, target_drive_id: str, target_parent_item_id: str, wid: int = 0):
    """
    使用 Graph API 复制单个 DriveItem（不用 SDK），并轮询进度。
    返回复制完成后的新 DriveItem（dict），否则返回 None。
    """
    url = f"https://graph.microsoft.com/v1.0/drives/{source_drive_id}/items/{source_item_id}/copy"
    headers = {
        "Authorization": f"Bearer {get_access_token(target_credential)}",
        "Content-Type": "application/json"
    }
    payload = {
        # 不指定 name 时沿用原名称
        "parentReference": {
            "driveId": target_drive_id,
            "id": target_parent_item_id
        },
        "@microsoft.graph.conflictBehavior": CONFLICT_BEHAVIOR
    }

    # 提交复制请求
    async with session.post(url, json=payload, headers=headers) as resp:
        if resp.status == 201:
            # 极少数情况下直接返回新项
            try:
                return await resp.json()
            except Exception:
                return None
        if resp.status != 202:
            detail = await resp.text()
            raise Exception(f"提交复制请求失败: {resp.status} {detail}")

        monitor_url = resp.headers.get("Location") or resp.headers.get("Azure-AsyncOperation")
        if not monitor_url:
            raise Exception("服务未返回进度监控 URL。")

        retry_after = resp.headers.get("Retry-After")

    # 轮询复制进度
    last_percent = -1.0
    last_status = None
    while True:
        async with session.get(monitor_url, headers={"Authorization": f"Bearer {get_access_token(target_credential)}"}) as mon:
            # 一些实现会在完成时返回 201/303 指向新资源
            if mon.status == 201:
                try:
                    return await mon.json()
                except Exception:
                    loc = mon.headers.get("Location")
                    if loc:
                        async with session.get(loc, headers={"Authorization": f"Bearer {get_access_token(target_credential)}"}) as got:
                            if got.status == 200:
                                return await got.json()
                    return None

            if mon.status == 303:
                loc = mon.headers.get("Location")
                if loc:
                    async with session.get(loc, headers={"Authorization": f"Bearer {get_access_token(target_credential)}"}) as got:
                        if got.status == 200:
                            return await got.json()
                return None

            if mon.status not in (200, 202):
                txt = await mon.text()
                if last_percent > 0 and mon.status == 401:
                    # 之前能查询到进度，突然变成401，说明不存在权限问题，直接返回完成状态
                    return {"status": "completed"}
                raise Exception(f"查询进度失败: uri: {monitor_url} {mon.status} {txt}")

            data = {}
            try:
                data = await mon.json()
            except Exception:
                data = {}

            status = (data.get("status") or "").lower()
            percent = data.get("percentageComplete") or data.get("progress") or data.get("percentComplete")
            try:
                percent = float(percent) if percent is not None else None
            except Exception:
                percent = None

            # 打印进度与状态（去抖动）
            printFlag = False
            if status and status != last_status:
                printFlag = True
                last_status = status
            if percent is not None and (last_percent < 0 or percent - last_percent >= 1.0 or percent >= 100.0):
                printFlag = True
                last_percent = percent
            # if printFlag:
            #     print(f"工作线程 {wid} 复制文件 {id2Name.get(f'{source_drive_id}:{source_item_id}', source_item_id)} , 状态: {status}, 进度: {(percent or 0):.0f}%")

            if status in ("completed", "success", "succeeded"):
                # 优先尝试资源定位
                loc = mon.headers.get("Location") or data.get("resourceLocation")
                if loc:
                    async with session.get(loc, headers={"Authorization": f"Bearer {get_access_token(target_credential)}"}) as got:
                        if got.status == 200:
                            return await got.json()
                res_id = data.get("resourceId")
                if res_id:
                    item_url = f"https://graph.microsoft.com/v1.0/drives/{target_drive_id}/items/{res_id}"
                    async with session.get(item_url, headers={"Authorization": f"Bearer {get_access_token(target_credential)}"}) as got:
                        if got.status == 200:
                            return await got.json()
                return {"status": "completed"}

            if status in ("failed", "cancelled", "canceled"):
                raise Exception(f"复制失败: {data}")

            # 间隔控制：优先使用服务端建议
            ra = mon.headers.get("Retry-After") or retry_after
            delay = float(ra) if ra else 1.5
            await asyncio.sleep(delay)


async def copy_files(target_client: GraphServiceClient, target_credential: InteractiveCredential, source_drive_id: str, source_item_id: str,
                     target_drive_id: str, target_parent_item_id: str):
    """
    遍历源 DriveItem 的所有子项，并将它们复制到目标路径下。
    """
    # 待处理的项列表
    items_to_process = [(source_item_id, target_parent_item_id)]
    # 目标父项的children缓存，避免重复拉取，只缓存文件夹类型的children
    target_children_cache = dict()

    queue = asyncio.Queue(maxsize= CONCURRENCY * 10)
    sem = asyncio.Semaphore(CONCURRENCY)
    stop_sentinel = object()

    session = aiohttp.ClientSession()

    async def worker(wid: int):
        while True:
            task = await queue.get()
            if task is stop_sentinel:
                print(f"工作线程 {wid} 收到停止信号，退出。")
                break
            source_item, target_parent_id = task
            async with sem:
                try:
                    print(f"工作线程 {wid} 提交复制任务: {getattr(source_item, 'name', source_item.id)} -> 目标父项 {id2Name.get(f'{target_drive_id}:{target_parent_id}', target_parent_id)}")
                    # 使用 api 进行复制，而非sdk，以获取复制进度
                    copied_item = await copy_item(session, target_credential, source_drive_id, getattr(source_item, 'id'), target_drive_id, target_parent_id, wid)
                    if copied_item:
                        print(f"工作线程 {wid} 复制成功 {getattr(source_item, 'name', source_item.id)} -> 新项 ID: {getattr(copied_item, 'id', '未知')}, 名称: {getattr(copied_item, 'name', '未知')}")
                    else:
                        print(f"工作线程 {wid} 复制操作未返回预期结果，请手动检查 {getattr(source_item, 'name', source_item.id)} -> 目标父项 {id2Name.get(f'{target_drive_id}:{target_parent_id}', target_parent_id)}")
    
                except Exception as e:
                    print(f"工作线程 {wid} 复制失败 {getattr(source_item, 'name', source_item.id)} -> 目标父项 {id2Name.get(f'{target_drive_id}:{target_parent_id}', target_parent_id)}: {e}")
                finally:
                    queue.task_done()

    # 启动工作线程
    workers = [asyncio.create_task(worker(i + 1)) for i in range(CONCURRENCY)]

    while items_to_process:
        current_source_id, current_target_parent_id = items_to_process.pop(0)

        # 获取当前源项的详细信息
        source_item = await target_client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(current_source_id).get()
        if not source_item or not getattr(source_item, "id", None):
            print(f"无法获取源项 ID '{current_source_id}' 名称 {id2Name.get(f'{source_drive_id}:{current_source_id}')} 的详细信息，跳过。")
            continue

        # 获取目标父项的children，用以确认目标父项的子项中是否存在同名文件夹
        target_parent_children = target_children_cache.get(current_target_parent_id, None)
        if target_parent_children is None:
            target_parent_children = dict()
            result = await target_client.drives.by_drive_id(target_drive_id).items.by_drive_item_id(current_target_parent_id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        if getattr(child, "folder", None):
                            target_parent_children[child.name] = child
                        id2Name[f'{target_drive_id}:{child.id}'] = child.name
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await target_client.drives.by_drive_id(target_drive_id).items.by_drive_item_id(current_target_parent_id).children.with_url(next_link).get()
            target_children_cache[current_target_parent_id] = target_parent_children

        # 情况梳理
        # 如果源项是文件，直接复制到目标父项下即可
        # 如果源项是文件夹，则需要判断目标父项下是否有同名文件夹
            # 无同名文件夹，则判断源项大小
                # 未超过阈值，则直接复制到目标父项下
                # 超过阈值，则在目标父项下新建同名文件夹，并遍历源项其子项，并判断子项类型
                    # 文件，直接复制到新建的同名文件夹下
                    # 文件夹，添加到待处理列表，等待处理
            # 有同名文件夹，则使用该同名文件夹作为目标父项，并遍历源项其子项，并判断子项类型
                # 文件，直接复制到该同名文件夹下
                # 文件夹，添加到待处理列表，等待处理

        to_copy_items = []
        # 需要遍历源项的子项flag
        need_traverse_children = False
        # 如果源项是文件，则直接添加到待复制列表
        if getattr(source_item, "file", None):
            to_copy_items.append((source_item, current_target_parent_id))
        # 如果源项是文件夹，则判断目标父项下是否有同名文件夹
        if getattr(source_item, "folder", None):
            target_child = target_parent_children.get(source_item.name, None)
            if not target_child:
                # 无同名文件夹，则判断源项大小
                if getattr(source_item, "size") <= MAX_COPY_SIZE:
                    to_copy_items.append((source_item, current_target_parent_id))
                else:
                    # 在目标父项下新建同名文件夹
                    target_child = await target_client.drives.by_drive_id(target_drive_id).items.by_drive_item_id(current_target_parent_id).children.post(DriveItem(name=source_item.name, folder=Folder()))
                    id2Name[f'{target_drive_id}:{getattr(target_child, "id")}'] = getattr(target_child, 'name')
                    need_traverse_children = True
            else:
                # 有同名文件夹
                need_traverse_children = True
        
        if need_traverse_children:
            # 遍历源项的子项
            result = await target_client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(current_source_id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        if getattr(child, 'file', None):
                            # 文件，直接添加到待复制列表
                            to_copy_items.append((child, getattr(target_child, 'id')))
                        if getattr(child, 'folder', None):
                            items_to_process.append((getattr(child, 'id'), getattr(target_child, 'id')))
                        id2Name[f'{source_drive_id}:{getattr(child, "id")}'] = getattr(child, 'name')
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await target_client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(current_source_id).children.with_url(next_link).get()
        
        # 使用aio并发复制待复制列表中的项
        if to_copy_items:
            for source_item, target_parent_id in to_copy_items:
                await queue.put((source_item, target_parent_id))
    
    for _ in range(CONCURRENCY * 2):
        await queue.put(stop_sentinel)
    
    await asyncio.gather(*workers, return_exceptions=True)
    await session.close()
    print("所有复制任务已提交并处理完毕。")


async def check_and_grant_permission(graph_client: GraphServiceClient, drive_id: str, item_id: str, target_mail: str):
    """
    检查指定用户是否对指定 DriveItem 拥有权限。如果没有，则赋予其读取权限。
    如果发起了赋权操作，则返回新的 Permission 对象，否则返回 None。
    """
    try:
        print(f"正在检查用户 '{target_mail}' 对项 ID '{id2Name.get(item_id, item_id)}' 的权限...")
                # 拉取该项的所有权限（包含分页）
        all_permissions = []
        result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).permissions.get()
        while True:
            if result and getattr(result, "value", None):
                all_permissions.extend(result.value or [])
            next_link = getattr(result, "odata_next_link", None) if result else None
            if not next_link:
                break
            result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).permissions.with_url(next_link).get()

        def identity_matches_email(identity, email):
            if not identity or not email:
                return False
            email_l = email.lower()
            user = getattr(identity, "user", None)
            if user:
                # IdentitySet.user.email
                if getattr(user, "email", None):
                    return str(user.email).lower() == email_l
                # IdentitySet.user.additional_data['email']
                if getattr(user, "additional_data", None) and user.additional_data.get('email', None):
                    return str(user.additional_data['email']).lower() == email_l
            # 某些模型可能直接有 email 字段
            if getattr(identity, "email", None):
                return str(identity.email).lower() == email_l
            return False

        def permission_for_email(perm, email):
            # 单个主体
            if identity_matches_email(getattr(perm, "granted_to_v2", None), email):
                return True
            if identity_matches_email(getattr(perm, "granted_to", None), email):
                return True
            # 多个主体
            for ident in getattr(perm, "granted_to_identities_v2", []) or []:
                if identity_matches_email(ident, email):
                    return True
            for ident in getattr(perm, "granted_to_identities", []) or []:
                if identity_matches_email(ident, email):
                    return True
            return False

        target_perms = [p for p in all_permissions if permission_for_email(p, target_mail)]
        has_read = any("read" in [str(r).lower() for r in (getattr(p, "roles", []) or [])] for p in target_perms)
        has_write = any("write" in [str(r).lower() for r in (getattr(p, "roles", []) or [])] for p in target_perms)

        # 如果既没有读也没有写权限，则赋予读取权限
        if not has_read and not has_write:
            body = InvitePostRequestBody(
                recipients=[DriveRecipient(email=target_mail)],
                require_sign_in=True,
                send_invitation=False,
                roles=["read"]
            )
            print(f"用户 '{target_mail}' 没有权限，正在赋予读取权限...")
            perms = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).invite.post(body)
            if perms and getattr(perms, "value", None):
                print(f"成功赋予用户 '{target_mail}' 读取权限。")
                return perms
            else:
                print(f"赋予权限操作未返回预期结果，请手动检查。")
                raise Exception("赋权失败")
        else:
            print(f"用户 '{target_mail}' 已有{['读', '写'][has_write]}权限，跳过赋权。")

        return None
        
    except Exception as e:
        print(f"检查或赋权时发生错误: {e}")
        raise e


async def get_drive_item_by_path(graph_client: GraphServiceClient, drive_id: str, path: str, auto_create: bool = False):
    """
    通过路径逐段遍历 children 来获取 DriveItem。如果auto_create为True，则在路径不存在时自动创建文件夹。
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
        source_credential = DeviceCodeCredential(client_id=CLIENT_ID)
        source_client = GraphServiceClient(credentials=source_credential, scopes=scopes)
        source_account = await source_client.me.get()
        source_mail = getattr(source_account, 'mail')
        print(f"已登录源账户: {source_mail}")
        source_drive = await source_client.me.drive.get()
        if not source_drive or not source_drive.id:
            print("无法获取源账户的 Drive 信息。请确保源账户有 OneDrive for Business。")
            return

        target_credential = DeviceCodeCredential(client_id=CLIENT_ID)
        target_client = GraphServiceClient(credentials=target_credential, scopes=scopes)
        target_account = await target_client.me.get()
        target_mail = getattr(target_account, 'mail')
        print(f"已登录目标账户: {target_mail}")
        target_drive = await target_client.me.drive.get()
        if not target_drive or not target_drive.id:
            print("无法获取目标账户的 Drive 信息。请确保目标账户有 OneDrive for Business。")
            return

        source_drive_id = source_drive.id
        target_drive_id = target_drive.id
        print(f"成功获取 Drive ID：源账户 {source_drive_id}，目标账户 {target_drive_id}")

        print("准备复制操作所需的认证信息，请再次登录目标账户...")
        get_access_token(target_credential) # 预热 token

    except Exception as e:
        print(f"发生错误: {e}")
        if "AADSTS700016" in str(e):
            print("认证错误: 应用标识符(CLIENT_ID)可能不正确或未在目标租户中正确配置。")
        elif "AADSTS900561" in str(e):
             print("认证错误: 设备代码认证流程未完成或已超时。")
        return

    try:
        print(f"正在查找源路径 '{SOURCE_PATH}' ...")
        source_item = await get_drive_item_by_path(source_client, source_drive_id, SOURCE_PATH)
        if not source_item or not source_item.id:
            print(f"找不到源路径: '{SOURCE_PATH}'")
            return
        
        print(f'找到源项 ID: {source_item.id}')

        print(f"正在查找或创建目标路径 '{TARGET_PARENT_PATH}' ...")
        target_parent_item = await get_drive_item_by_path(target_client, target_drive_id, TARGET_PARENT_PATH, auto_create=True)
        if not target_parent_item or not target_parent_item.id:
            print(f"无法找到或创建目标路径: '{TARGET_PARENT_PATH}'")
            return

        print(f'找到目标项 ID: {target_parent_item.id}')
    
    except Exception as e:
        print(f"查找路径时发生错误: {e}")
        return

    try:
        perms = await check_and_grant_permission(source_client, source_drive_id, source_item.id, target_mail)

        await copy_files(target_client, target_credential, source_drive_id, source_item.id, target_drive_id, target_parent_item.id)
        
        # 暂时不撤销权限，让用户手动去撤销
        # if perms and getattr(perms, "value", None):
        #     perm_list = perms.value or []
        #     print(f"正在撤销临时赋予的{len(perm_list)}个权限...")
        #     for p in perm_list:
        #         perm_id = getattr(p, "id", None)
        #         if perm_id:
        #             await source_client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(source_item.id).permissions.by_permission_id(perm_id).delete()
        #             print(f"已撤销权限 ID: {perm_id}")

    except Exception as e:
        print(f"拷贝文件时发生错误: {e}")


if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行两次认证。第一次是源账户，第二次是目标账户。")
    asyncio.run(main())
