package gedis

var (
	ErrKeyFormat = NewError(`key format error`)
	ErrKeyList   = NewError(`key list is empty`)
)

type GedisError struct {
	msg string
}

func NewError(msg string) error {
	return &GedisError{msg: msg}
}

func (ge *GedisError) Error() string {
	return ge.msg
}

func (ge *GedisError) Is(target error) bool {
	gde, ok := target.(*GedisError)
	if !ok {
		return false
	}

	return ge.msg == gde.msg
}
