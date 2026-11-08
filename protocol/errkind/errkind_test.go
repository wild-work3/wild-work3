package errkind

import "testing"

// TestRotatableRequestLevel 锁定「请求级错误不可换号」的语义。
//
// 拦截器 request.prepare 的 fail-closed 拒绝（如 EC 硬预算门）返回 prompt_too_long，
// 它必须不可换号——换一个账号必然撞同一面墙（上下文超限是请求体问题，与账号无关）。
// 同理 image_invalid / bad_params / content_blocked / passthrough 都是请求级。
func TestRotatableRequestLevel(t *testing.T) {
	for _, k := range []Kind{PromptTooLong, ImageInvalid, BadParams, ContentBlocked, Passthrough} {
		if Rotatable(k) {
			t.Errorf("%s 是请求级错误，不应换号（换号必然复现）", k)
		}
	}
}

// TestRotatableAccountLevel 锁定「账号级错误应换号」的语义（回归护栏）。
func TestRotatableAccountLevel(t *testing.T) {
	for _, k := range []Kind{SoftRate, HardCredit, SessionDead, NotFound, AccountFault, Client, Queued, Auth} {
		if !Rotatable(k) {
			t.Errorf("%s 是账号级错误，应换号", k)
		}
	}
}
